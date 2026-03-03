"""Resumable GitHub issues sync pipeline.

Split into two decoupled stages connected by an in-memory queue:

- **IssueFetcher** (background thread): replays cached issues then fetches
  new ones from the GitHub API, pushing dicts onto the queue.
- **Ingester** (thread pool): reads from the queue and upserts into ChromaDB.

A shared ``StopSignal`` carries the first error from either side and stops both.
"""

import json
import queue
import threading
from collections.abc import Iterator
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
from datetime import datetime
from pathlib import Path

from ghrag import get_cache_dir
from ghrag.github import get_github_token, issue_to_dict, issue_to_document


# ---------------------------------------------------------------------------
# StopSignal
# ---------------------------------------------------------------------------

class StopSignal:
    """Shared stop flag between fetcher and ingester.

    The first error wins.  Both sides check ``is_set()`` to bail out.
    """

    def __init__(self):
        self._event = threading.Event()
        self.error: BaseException | None = None

    def stop(self, error: BaseException | None = None):
        if self.error is None:
            self.error = error
        self._event.set()

    def is_set(self) -> bool:
        return self._event.is_set()

    def raise_if_error(self):
        if self.error is not None:
            raise self.error


# ---------------------------------------------------------------------------
# Progress
# ---------------------------------------------------------------------------

class Progress:
    """Thread-safe progress counter shared between fetcher and ingester."""

    _CLEAR = "\r\033[K"  # carriage return + clear to end of line

    def __init__(self):
        self._lock = threading.Lock()
        self.total: int = 0
        self.done: int = 0
        self._active: bool = False  # whether a \r progress line is showing

    def add_total(self, n: int):
        with self._lock:
            self.total += n
            self._print_bar()

    def log(self, message: str):
        """Print a status message without breaking the progress line."""
        with self._lock:
            if self._active:
                print(f"{self._CLEAR}{message}", flush=True)
            else:
                print(message, flush=True)
            self._print_bar()

    def advance(self):
        with self._lock:
            self.done += 1
            self._print_bar()

    def _print_bar(self):
        """Print the progress bar (must be called with lock held)."""
        if self.total > 0:
            self._active = True
            print(f"\rIngested {self.done}/{self.total} issues", end="", flush=True)

    def finish(self):
        if self._active:
            print()


# ---------------------------------------------------------------------------
# IssuesCache
# ---------------------------------------------------------------------------

class IssuesCache:
    """Dict of issues backed by a JSONL file.

    Deduplicates by issue number.  ``save()`` rewrites the file.
    Only the fetch thread touches this — no locking needed.
    """

    def __init__(self, jsonl_path: Path):
        self._path = jsonl_path
        self._issues: dict[int, dict] = {}
        if jsonl_path.exists():
            for line in open(jsonl_path):
                line = line.strip()
                if line:
                    issue = json.loads(line)
                    self._issues[issue["number"]] = issue

    def put(self, issue: dict):
        self._issues[issue["number"]] = issue

    def values(self) -> list[dict]:
        return list(self._issues.values())

    def save(self):
        with open(self._path, "w") as f:
            for issue in self._issues.values():
                f.write(json.dumps(issue) + "\n")


# ---------------------------------------------------------------------------
# IssueFetcher
# ---------------------------------------------------------------------------

class IssueFetcher:
    """Fetches issues from GitHub, replaying from cache first.

    Manages its own background thread.  Checks ``stop.is_set()`` to bail
    out early; calls ``stop.stop(exc)`` on error.
    """

    def __init__(self, repo: str, cache_dir: Path, stop: StopSignal, progress: Progress):
        self.repo = repo
        self.stop = stop
        self.progress = progress
        self._cache_meta_path = cache_dir / "cache_last_update.txt"
        self.cache = IssuesCache(cache_dir / "issues.jsonl")

    # -- metadata -----------------------------------------------------------

    @property
    def cache_last_update(self) -> str | None:
        if self._cache_meta_path.exists():
            return self._cache_meta_path.read_text().strip() or None
        return None

    @cache_last_update.setter
    def cache_last_update(self, value: str):
        self._cache_meta_path.write_text(value)

    # -- public API ---------------------------------------------------------

    def start(self, since: str | None, q: queue.Queue) -> threading.Thread:
        """Start fetching in a background thread.

        Puts issue dicts onto *q*, sends ``None`` sentinel when done.
        On error, calls ``self.stop.stop(exc)``.  Always saves cache.

        Returns the thread so the caller can join it.
        """

        def _run():
            try:
                for issue in self._iter_issues(since):
                    if self.stop.is_set():
                        break
                    q.put(issue)
            except BaseException as exc:
                self.stop.stop(exc)
            finally:
                self.cache.save()
                q.put(None)  # sentinel — always sent

        t = threading.Thread(target=_run)
        t.start()
        return t

    # -- internals ----------------------------------------------------------

    def _iter_issues(self, since: str | None) -> Iterator[dict]:
        """Yield all issues with ``updated_at >= since``.

        1. Yield matching issues already in the JSONL cache.
        2. Fetch from GitHub API starting at ``cache_last_update``
           (which may be earlier than *since*), cache each, and yield
           those with ``updated_at >= since``.
        """
        if since is not None:
            cutoff = datetime.fromisoformat(since).timestamp()
        else:
            cutoff = None

        # 1. Replay from cache
        cached = self.cache.values()
        if cached:
            matching = [
                i for i in cached
                if cutoff is None
                or datetime.fromisoformat(i["updated_at"]).timestamp() > cutoff
            ]
            if matching:
                self.progress.add_total(len(matching))
                self.progress.log(f"Replaying {len(matching)} issues from cache.")
                yield from matching

        # 2. Fetch new issues from GitHub API
        from github import Auth, Github

        token = get_github_token()
        g = Github(auth=Auth.Token(token))
        github_repo = g.get_repo(self.repo)

        cache_last_update = self.cache_last_update
        if cache_last_update:
            self.progress.log(f"Fetching updates since {cache_last_update}...")
            api_since = datetime.fromisoformat(cache_last_update)
            issues_iter = github_repo.get_issues(
                state="all", sort="updated", direction="asc", since=api_since,
            )
        else:
            self.progress.log("Fetching all issues...")
            issues_iter = github_repo.get_issues(
                state="all", sort="updated", direction="asc",
            )

        for issue in issues_iter:
            if self.stop.is_set():
                break
            issue_dict = issue_to_dict(issue)
            self.cache.put(issue_dict)
            self.cache_last_update = issue_dict["updated_at"]

            if cutoff is None or datetime.fromisoformat(issue_dict["updated_at"]).timestamp() > cutoff:
                self.progress.add_total(1)
                yield issue_dict


# ---------------------------------------------------------------------------
# Ingester
# ---------------------------------------------------------------------------

class Ingester:
    """Reads from a queue and upserts into ChromaDB using a thread pool.

    Checks ``stop.is_set()`` to bail out; calls ``stop.stop(exc)`` on first
    worker error.
    """

    def __init__(self, cache_dir: Path, store_type: str, stop: StopSignal, progress: Progress, num_workers: int = 4):
        self.stop = stop
        self.progress = progress
        self.num_workers = num_workers
        self._store_meta_path = cache_dir / f"store_last_update_{store_type}.txt"

    # -- metadata -----------------------------------------------------------

    @property
    def store_last_update(self) -> str | None:
        if self._store_meta_path.exists():
            return self._store_meta_path.read_text().strip() or None
        return None

    @store_last_update.setter
    def store_last_update(self, value: str):
        self._store_meta_path.write_text(value)

    # -- public API ---------------------------------------------------------

    def ingest_queue(self, q: queue.Queue, store):
        """Drain *q* and upsert each issue into *store*.

        Blocks until the ``None`` sentinel or ``stop`` is set.
        """
        self._store = store
        futures = []
        pool = ThreadPoolExecutor(max_workers=self.num_workers)

        def on_done(f):
            if f.cancelled():
                return
            exc = f.exception()
            if exc is not None:
                self.stop.stop(exc)

        try:
            while not self.stop.is_set():
                try:
                    item = q.get(timeout=0.5)
                except queue.Empty:
                    continue
                if item is None:
                    break
                fut = pool.submit(self._ingest_one, item)
                fut.add_done_callback(on_done)
                futures.append(fut)
        finally:
            if self.stop.is_set():
                for f in futures:
                    f.cancel()
            elif futures:
                _, pending = wait(futures, return_when=FIRST_EXCEPTION)
                for f in pending:
                    f.cancel()
            pool.shutdown(wait=True)

    # -- internals ----------------------------------------------------------

    def _ingest_one(self, issue: dict):
        """Upsert a single issue into the vector store."""
        doc = issue_to_document(issue)
        self._store.upsert(doc)
        self.store_last_update = issue["updated_at"]
        self.progress.advance()


# ---------------------------------------------------------------------------
# Top-level wiring
# ---------------------------------------------------------------------------

def sync(repo: str, store_type: str = "duckdb", force: bool = False, num_workers: int = 4):
    """Run a full sync: fetch issues from GitHub and ingest into the vector store."""
    from ghrag.store import create_store

    cache_dir = get_cache_dir(repo)

    if force:
        for name in ("cache_last_update.txt", "issues.jsonl", f"store_last_update_{store_type}.txt"):
            p = cache_dir / name
            if p.exists():
                p.unlink()

    stop = StopSignal()
    progress = Progress()

    fetcher = IssueFetcher(repo, cache_dir, stop, progress)
    ingester = Ingester(cache_dir, store_type, stop, progress, num_workers)

    store = create_store(repo, cache_dir, store_type)

    since = ingester.store_last_update
    q: queue.Queue[dict | None] = queue.Queue()

    fetch_thread = fetcher.start(since, q)
    try:
        ingester.ingest_queue(q, store)  # blocks until sentinel or stop
    except KeyboardInterrupt:
        stop.stop()
    finally:
        fetch_thread.join()
        progress.finish()

    stop.raise_if_error()

    if store_type == "duckdb":
        print("Building index...")
        store.build_index()

    print(f"Done! Store contains {store.size()} documents.")
