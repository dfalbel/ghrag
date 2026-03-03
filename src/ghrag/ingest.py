from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import heapq
import json
import queue
import threading
import raghilda

class Event:
    class Fetch:
        @dataclass(frozen=True)
        class Ok: issue: dict
        @dataclass(frozen=True)
        class Done: pass
    class Ingest:
        @dataclass(frozen=True)
        class Ok: issue: dict
    @dataclass(frozen=True)
    class Error:
        exp: Exception
        issue: dict | None = None
    @dataclass(frozen=True)
    class Done: pass

class Inbox:
    """Thread-safe inbox for the fetch→ingest pipeline.

    Pop priority:
      1. Errors (Event.Error)
      2. Ingestion confirmations (Event.Ingest.Ok)
      3. Fetched issues, oldest first (Event.Fetch.Ok)
      4. Done — returned only when Event.Fetch.Done was seen and all
         ingestion results have been received.
    """

    def __init__(self):
        self._cond = threading.Condition()
        self._errors: list[Event.Error] = []
        self._fetch_issues: list[tuple[str, int, Event.Fetch.Ok]] = []  # min-heap
        self._ingest_results: list[Event.Ingest.Ok] = []
        self._fetch_done: bool = False
        self._pending: int = 0  # Event.Fetch.Ok put − (Event.Ingest.Ok + Event.Error w/ issue) put

    def put(self, item):
        with self._cond:
            match item:
                case Event.Fetch.Ok():
                    heapq.heappush(
                        self._fetch_issues,
                        (item.issue["updated_at"], item.issue["number"], item),
                    )
                    self._pending += 1
                case Event.Fetch.Done():
                    self._fetch_done = True
                case Event.Ingest.Ok():
                    self._ingest_results.append(item)
                    self._pending -= 1
                case Event.Error() if item.issue is not None:
                    self._errors.append(item)
                    self._pending -= 1
                case Event.Error():
                    self._errors.append(item)
            self._cond.notify()

    def _ready(self) -> bool:
        return bool(
            self._errors
            or self._ingest_results
            or self._fetch_issues
            or (self._fetch_done and self._pending == 0)
        )

    def pop(self):
        """Block until an item is available."""
        with self._cond:
            while not self._ready():
                self._cond.wait()
            if self._errors:
                return self._errors.pop(0)
            if self._ingest_results:
                return self._ingest_results.pop(0)
            if self._fetch_issues:
                _, _, item = heapq.heappop(self._fetch_issues)
                return item
            return Event.Done()

class IssuesCache:
    """Issue cache backed by JSONL and deduped by issue number."""
    def __init__(self, jsonl_path: Path):
        self._path = jsonl_path
        self._issues: dict[int, dict] = {}
        if jsonl_path.exists():
            with open(jsonl_path) as file:
                for line in file:
                    line = line.strip()
                    if not line:
                        continue
                    issue = json.loads(line)
                    self._issues[issue["number"]] = issue

    def put(self, issue: dict):
        self._issues[issue["number"]] = issue

    def values(self) -> list[dict]:
        return list(self._issues.values())

    def save(self):
        with open(self._path, "w") as file:
            for issue in self._issues.values():
                file.write(json.dumps(issue) + "\n")

class Fetcher:
    def __init__(self, repo: str, cache_dir: Path, since: datetime | None):
        self._repo = repo
        self._cache_meta_path = cache_dir / "cache_last_update.txt"
        self._cache = IssuesCache(cache_dir / "issues.jsonl")
        self._since = since
        self._thread: threading.Thread | None = None
        self._last_updated_at: str | None = None

    def start(self, inbox: Inbox):
        """
        Loads cached issues synchronously, then starts a background thread
        to fetch new issues from Github.
        """
        self._replay_cache(inbox)

        def _run():
            try:
                self._fetch_github(inbox)
            except Exception as exc:
                inbox.put(Event.Error(exp=exc))
            finally:
                inbox.put(Event.Fetch.Done())

        self._thread = threading.Thread(target=_run, name="issue-fetcher")
        self._thread.start()

    def _replay_cache(self, inbox: Inbox):
        matching = [
            issue for issue in self._cache.values()
            if self._since is None
            or datetime.fromisoformat(issue["updated_at"]) > self._since
        ]
        for issue in matching:
            inbox.put(Event.Fetch.Ok(issue=issue))

    def _fetch_github(self, inbox: Inbox):
        from github import Auth, Github
        from ghrag.github import get_github_token, issue_to_dict

        token = get_github_token()
        g = Github(auth=Auth.Token(token))
        github_repo = g.get_repo(self._repo)

        cache_last_update = self._cache_meta_path.read_text().strip() if self._cache_meta_path.exists() else None
        if cache_last_update:
            issues_iter = github_repo.get_issues(
                state="all", sort="updated", direction="asc",
                since=datetime.fromisoformat(cache_last_update),
            )
        else:
            issues_iter = github_repo.get_issues(
                state="all", sort="updated", direction="asc",
            )

        for issue in issues_iter:
            issue_dict = issue_to_dict(issue)
            self._cache.put(issue_dict)
            self._last_updated_at = issue_dict["updated_at"]

            if self._since is None or datetime.fromisoformat(issue_dict["updated_at"]) > self._since:
                inbox.put(Event.Fetch.Ok(issue=issue_dict))

    def stop(self):
        """
        When stop is called, we make sure the cache is stored and the last date
        is updated in the disk cache.
        """
        if self._thread is not None:
            self._thread.join()
        self._cache.save()
        if self._last_updated_at is not None:
            self._cache_meta_path.write_text(self._last_updated_at)


class Ingester:

    _SENTINEL = object()

    def __init__(self, cache_dir: Path, store_type: str, store: raghilda.store.BaseStore, num_workers: int = 4):
        self._store = store
        self._num_workers = num_workers
        self._store_meta_path = cache_dir / f"store_last_update_{store_type}.txt"
        self._queue: queue.Queue = queue.Queue(maxsize=num_workers)
        self._workers: list[threading.Thread] = []
        self._last_updated_at: str | None = None
        self._lock = threading.Lock()
        self._inbox: Inbox | None = None

    def start(self, inbox: Inbox):
        """
        Starts the ingestion threads. Shouldn't be able to submit without starting.
        """
        if self._workers:
            raise RuntimeError("Ingester already started")
        self._inbox = inbox
        for i in range(self._num_workers):
            t = threading.Thread(target=self._worker, name=f"ingester-{i}")
            t.start()
            self._workers.append(t)

    def submit(self, issue: dict):
        """
        Submit new work to the ingester pool. Blocks if we are already processing > num_workers tasks.
        Which allows us to easily stop later.
        """
        if not self._workers:
            raise RuntimeError("Ingester not started")
        self._queue.put(issue)

    def stop(self):
        """
        Finishes processing the internal queue and closes the ingesting threads.
        Writes the update_date of the last ingested issue.
        """
        for _ in self._workers:
            self._queue.put(self._SENTINEL)
        for t in self._workers:
            t.join()
        self._workers.clear()
        if self._last_updated_at is not None:
            self._store_meta_path.write_text(self._last_updated_at)

    def _worker(self):
        from ghrag.github import issue_to_document
        while True:
            item = self._queue.get()
            if item is self._SENTINEL:
                break
            try:
                doc = issue_to_document(item)
                self._store.upsert(doc)
                with self._lock:
                    if self._last_updated_at is None or item["updated_at"] > self._last_updated_at:
                        self._last_updated_at = item["updated_at"]
                self._inbox.put(Event.Ingest.Ok(issue=item))
            except Exception as exc:
                self._inbox.put(Event.Error(exp=exc, issue=item))


def sync(repo: str, store_type: str = "duckdb", force: bool = False, num_workers: int = 4):
    from ghrag import get_cache_dir
    from ghrag.store import create_store

    cache_dir = get_cache_dir(repo)

    store_meta = cache_dir / f"store_last_update_{store_type}.txt"

    if force:
        for name in ("cache_last_update.txt", "issues.jsonl", f"store_last_update_{store_type}.txt"):
            p = cache_dir / name
            if p.exists():
                p.unlink()

    store = create_store(repo, cache_dir, store_type)
    inbox = Inbox()

    # Read the last ingested date so we only fetch updates since then.
    since = None
    if store_meta.exists():
        raw = store_meta.read_text().strip()
        if raw:
            since = datetime.fromisoformat(raw)

    fetcher = Fetcher(repo, cache_dir, since)
    ingester = Ingester(cache_dir, store_type, store, num_workers)

    fetcher.start(inbox)
    ingester.start(inbox)

    fetched = 0
    ingested = 0
    try:
        while True:
            event = inbox.pop()
            match event:
                case Event.Fetch.Ok(issue=issue):
                    fetched += 1
                    print(f"\rFetched {fetched} | Ingested {ingested}", end="", flush=True)
                    ingester.submit(issue)
                case Event.Ingest.Ok():
                    ingested += 1
                    print(f"\rFetched {fetched} | Ingested {ingested}", end="", flush=True)
                case Event.Error(exp=exc):
                    raise exc
                case Event.Done():
                    break
    except KeyboardInterrupt:
        pass
    finally:
        ingester.stop()
        fetcher.stop()
        if fetched > 0:
            print()

    if store_type == "duckdb":
        print("Building index...")
        store.build_index()

    print(f"Done! Store contains {store.size()} documents.")
