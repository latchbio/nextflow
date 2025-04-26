#!/Users/ayush/Desktop/core/nucleus-workflows/.venv/bin/python

import asyncio
import json
import sys
from dataclasses import dataclass
from typing import Literal

from latch_config.config import PostgresConnectionConfig, read_config
from latch_data_validation.data_validation import untraced_validate
from latch_postgres.postgres import (
    LatchAsyncConnection,
    get_pool,
    get_with_conn_retry,
    sqlq,
)

config = read_config(PostgresConnectionConfig, "db_")

pool = get_pool(config, "nextflow_forch_test", read_only=False)
with_conn_retry = get_with_conn_retry(pool, config)


@dataclass
class TaskInfo:
    id: int


@dataclass
class CreateTaskInput:
    display_name: str
    container_image: str
    container_entrypoint: list[str]
    cpus: int
    memory_bytes: int
    gpu_type: str | None
    gpus: int


async def create_task(payload: CreateTaskInput):
    @with_conn_retry
    async def db_work(conn: LatchAsyncConnection):
        return await conn.query1(
            TaskInfo,
            sqlq(
                """
                insert into
                    forch_pub.tasks(
                        display_name,
                        container_image,
                        container_entrypoint,
                        dedicated_cpuset_size,
                        dedicated_memory_bytes,
                        allow_internet_egress,
                        dedicated_gpu_type,
                        dedicated_gpu_count
                    )
                values
                    (
                        %(display_name)s,
                        %(container_image)s,
                        %(container_entrypoint)s,
                        %(cpus)s,
                        %(memory_bytes)s,
                        true,
                        %(gpu_type)s,
                        %(gpus)s
                    )
                returning
                    id
                """,
            ),
            display_name=payload.display_name,
            container_image=payload.container_image,
            container_entrypoint=payload.container_entrypoint,
            cpus=payload.cpus,
            memory_bytes=payload.memory_bytes,
            gpu_type=payload.gpu_type,
            gpus=payload.gpus,
        )

    return await db_work()


@dataclass
class TaskStatus:
    status: Literal[
        "queued",
        "initializing",
        "running",
        "succeeded",
        "failed",
    ]


async def get_task_status(task_id: int):
    @with_conn_retry
    async def db_work(conn: LatchAsyncConnection):
        return await conn.query1(
            TaskStatus,
            sqlq(
                """
                select
                    coalesce(
                        (
                            select
                                    (
                                        select
                                        case
                                            when te.type = 'node-assigned' then
                                                'submitted'
                                            when te.type = 'container-created' then
                                                'running'
                                            when te.type = 'container-exited' then
                                                (
                                                    select
                                                    case
                                                        when teced.exit_status = 0 then
                                                            'succeeded'
                                                        else
                                                            'failed'
                                                    end
                                                )
                                            else
                                                null
                                        end
                                    )
                            from
                                forch_pub.task_events te
                            left join
                                forch_pub.task_event_container_exited_data teced
                                on teced.id = te.id
                            where
                                te.task_id = %(task_id)s
                            order by
                                time desc
                            limit 1
                        ),
                        'queued'
                    ) status
                """,
            ),
            task_id=task_id,
        )

    return await db_work()


@dataclass
class TaskExitCode:
    exit_status: int | None


async def get_task_exit_code(task_id: int):
    @with_conn_retry
    async def db_work(conn: LatchAsyncConnection):
        return await conn.query1(
            TaskExitCode,
            sqlq(
                """
                select
                    teced.exit_status
                from
                    forch_pub.task_events te
                inner join
                    forch_pub.task_event_container_exited_data teced
                    on teced.id = te.id
                where
                    te.task_id = %(task_id)s
                """,
            ),
            task_id=task_id,
        )

    return await db_work()


async def main():
    await pool.open()
    args = sys.argv[1:]
    if len(args) != 2:
        print("Must provide a command")
        sys.exit(1)

    if args[0] == "create":
        payload = untraced_validate(json.loads(args[1]), CreateTaskInput)
        res = await create_task(payload)
        print(res.id)
    elif args[0] == "status":
        task_id = int(args[1])
        res = await get_task_status(task_id)
        print(res.status)
    elif args[0] == "exitcode":
        task_id = int(args[1])
        res = await get_task_exit_code(task_id)
        print(res.exit_status)


if __name__ == "__main__":
    asyncio.run(main())
