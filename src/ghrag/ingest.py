from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import heapq
import json
import logging
import queue
import threading
import raghilda

logger = logging.getLogger(__name__)

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

class Progress:
    """Thread-safe progress counters, updated as events arrive."""

    def __init__(self):
        self._lock = threading.Lock()
        self.fetched: int = 0
        self.ingested: int = 0

    def on_fetch(self):
        with self._lock:
            self.fetched += 1
            self._print()

    def on_ingest(self):
        with self._lock:
            self.ingested += 1
            self._print()

    def _print(self):
        print(f"\rFetched {self.fetched} | Ingested {self.ingested}", end="", flush=True)


class Inbox:
    """Thread-safe inbox for the fetch→ingest pipeline.

    Pop priority:
      1. Errors (Event.Error)
      2. Ingestion confirmations (Event.Ingest.Ok)
      3. Fetched issues, oldest first (Event.Fetch.Ok)
      4. Done — returned only when Event.Fetch.Done was seen and all
         ingestion results have been received.
    """

    def __init__(self, progress: Progress | None = None):
        self._cond = threading.Condition()
        self._errors: list[Event.Error] = []
        self._fetch_issues: list[tuple[str, int, Event.Fetch.Ok]] = []  # min-heap
        self._ingest_results: list[Event.Ingest.Ok] = []
        self._fetch_done: bool = False
        self._pending: int = 0  # Event.Fetch.Ok put − (Event.Ingest.Ok + Event.Error w/ issue) put
        self._progress = progress

    def put(self, item):
        with self._cond:
            match item:
                case Event.Fetch.Ok():
                    logger.debug("Inbox: fetched issue #%d (updated %s)", item.issue["number"], item.issue["updated_at"])
                    heapq.heappush(
                        self._fetch_issues,
                        (item.issue["updated_at"], item.issue["number"], item),
                    )
                    self._pending += 1
                    if self._progress:
                        self._progress.on_fetch()
                case Event.Fetch.Done():
                    logger.debug("Inbox: fetch done")
                    self._fetch_done = True
                case Event.Ingest.Ok():
                    logger.debug("Inbox: ingested issue #%d", item.issue["number"])
                    self._ingest_results.append(item)
                    self._pending -= 1
                    if self._progress:
                        self._progress.on_ingest()
                case Event.Error() if item.issue is not None:
                    logger.debug("Inbox: error for issue #%d: %s", item.issue["number"], item.exp)
                    self._errors.append(item)
                    self._pending -= 1
                case Event.Error():
                    logger.debug("Inbox: error (no issue): %s", item.exp)
                    self._errors.append(item)
            self._cond.notify()

    def _ready(self) -> bool:
        # True when any queue has items to drain, or when the pipeline is
        # fully settled (fetching finished and every fetched issue has been
        # ingested or errored out), meaning we can emit Done.
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
                item = self._errors.pop(0)
                logger.debug("Inbox.pop: error event")
                return item
            if self._ingest_results:
                item = self._ingest_results.pop(0)
                logger.debug("Inbox.pop: ingest ok #%d", item.issue["number"])
                return item
            if self._fetch_issues:
                _, _, item = heapq.heappop(self._fetch_issues)
                logger.debug("Inbox.pop: fetch ok #%d", item.issue["number"])
                return item
            logger.debug("Inbox.pop: done (pending=%d, fetch_done=%s)", self._pending, self._fetch_done)
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
        logger.debug("Fetcher: replaying %d cached issues (since=%s, total cached=%d)", len(matching), self._since, len(self._cache.values()))
        for issue in matching:
            inbox.put(Event.Fetch.Ok(issue=issue))

    def _fetch_github(self, inbox: Inbox):
        from github import Auth, Github
        from ghrag.github import get_github_token, issue_to_dict

        token = get_github_token()
        g = Github(auth=Auth.Token(token))
        github_repo = g.get_repo(self._repo)

        cache_last_update = self._cache_meta_path.read_text().strip() if self._cache_meta_path.exists() else None
        logger.debug("Fetcher: fetching from GitHub (cache_last_update=%s, since=%s)", cache_last_update, self._since)
        if cache_last_update:
            issues_iter = github_repo.get_issues(
                state="all", sort="updated", direction="asc",
                since=datetime.fromisoformat(cache_last_update),
            )
        else:
            issues_iter = github_repo.get_issues(
                state="all", sort="updated", direction="asc",
            )

        count = 0
        for issue in issues_iter:
            issue_dict = issue_to_dict(issue)
            self._cache.put(issue_dict)
            self._last_updated_at = issue_dict["updated_at"]
            count += 1

            if self._since is None or datetime.fromisoformat(issue_dict["updated_at"]) > self._since:
                inbox.put(Event.Fetch.Ok(issue=issue_dict))

        logger.debug("Fetcher: fetched %d issues from GitHub", count)

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

    def __init__(self, store: raghilda.store.BaseStore, store_meta_path: Path, num_workers: int = 4):
        self._store = store
        self._num_workers = num_workers
        self._store_meta_path = store_meta_path
        self._queue: queue.Queue = queue.Queue()
        self._stop_event = threading.Event()
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
        """Submit new work to the ingester pool."""
        if not self._workers:
            raise RuntimeError("Ingester not started")
        self._queue.put(issue)

    def stop(self):
        """
        Signal workers to stop, join them, and write the last update date.
        Workers finish their current item then exit.
        """
        self._stop_event.set()
        for t in self._workers:
            t.join()
        self._workers.clear()
        if self._last_updated_at is not None:
            self._store_meta_path.write_text(self._last_updated_at)

    def _worker(self):
        from ghrag.github import issue_to_document
        name = threading.current_thread().name
        logger.debug("Ingester: worker %s started", name)
        while not self._stop_event.is_set():
            try:
                item = self._queue.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                logger.debug("Ingester: %s processing issue #%d", name, item["number"])
                doc = issue_to_document(item)
                self._store.upsert(doc)
                logger.debug("Ingester: %s processed issue #%d", name, item["number"])
                with self._lock:
                    if self._last_updated_at is None or item["updated_at"] > self._last_updated_at:
                        self._last_updated_at = item["updated_at"]
                self._inbox.put(Event.Ingest.Ok(issue=item))
            except Exception as exc:
                logger.debug("Ingester: %s error on issue #%d: %s", name, item["number"], exc)
                self._inbox.put(Event.Error(exp=exc, issue=item))
        logger.debug("Ingester: worker %s stopped", name)


def sync(repo: str, store_type: str = "duckdb", force: bool = False, num_workers: int = 4):
    from ghrag import get_cache_dir
    from ghrag.store import create_store

    logger.debug("sync: repo=%s store_type=%s force=%s num_workers=%d", repo, store_type, force, num_workers)

    cache_dir = get_cache_dir(repo)
    logger.debug("sync: cache_dir=%s", cache_dir)

    store_meta = cache_dir / f"store_last_update_{store_type}.txt"

    if force:
        for name in ("cache_last_update.txt", "issues.jsonl", f"store_last_update_{store_type}.txt"):
            p = cache_dir / name
            if p.exists():
                p.unlink()

    store = create_store(repo, cache_dir, store_type)
    progress = Progress()
    inbox = Inbox(progress)

    # Read the last ingested date so we only fetch updates since then.
    since = None
    if store_meta.exists():
        raw = store_meta.read_text().strip()
        if raw:
            since = datetime.fromisoformat(raw)
    logger.debug("sync: since=%s", since)

    fetcher = Fetcher(repo, cache_dir, since)
    ingester = Ingester(store, store_meta, num_workers)

    fetcher.start(inbox)
    ingester.start(inbox)

    try:
        while True:
            event = inbox.pop()
            match event:
                case Event.Fetch.Ok(issue=issue):
                    ingester.submit(issue)
                case Event.Ingest.Ok():
                    pass
                case Event.Error(exp=exc):
                    raise exc
                case Event.Done():
                    break
    except KeyboardInterrupt:
        pass
    finally:
        ingester.stop()
        fetcher.stop()
        if progress.fetched > 0:
            print()

    if store_type == "duckdb":
        print("Building index...")
        store.build_index()

    print(f"Done! Store contains {store.size()} documents.")
