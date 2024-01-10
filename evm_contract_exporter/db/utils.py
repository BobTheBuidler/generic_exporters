
import pony.orm
from a_sync import AsyncThreadPoolExecutor
from y._db.common import retry_locked


read_threads = AsyncThreadPoolExecutor(16, thread_name_prefix="evm_contract_exporter__read_thread")
write_threads = AsyncThreadPoolExecutor(16, thread_name_prefix="evm_contract_exporter__write_thread")

db_session = lambda fn: retry_locked(pony.orm.db_session(fn))
