import itertools
import json
import os
import uuid
from datetime import datetime
from typing import Any

import boto3
import numpy as np
from sqlalchemy import delete, select, text, update
from sqlalchemy.util._collections import immutabledict
from ta_similarity.similarity_task.similarity_functions import load_faiss_and_dict, save_faiss_and_dict

from doc_delete_job.utils.utils import get_manage_faiss_path
from ta_db_connection.atatables import (
    Matches,
    MicroServicesSimilarCases,
    MicroServicesStatus,
    SentenceMatchesTitan,
    SentencesVector,
    SimilarCasesEmb,
    TextSimilarityInput,
)
from ta_db_connection.database_connection import DatabaseConnection
from ta_utils.logger.CustomLogger import CustomLogger

logger = CustomLogger("doc-delete-job")


def select_deletion_batch(session, analysis_group, batch_size, after_tsp_id=None):
    query = (
        select(TextSimilarityInput.tsp_id)
        .where(
            TextSimilarityInput.document_state == "READY_FOR_DELETION",
            TextSimilarityInput.analysis_group == f"{analysis_group}",
        )
        .order_by(TextSimilarityInput.tsp_id)
        .limit(batch_size)
    )
    if after_tsp_id is not None:
        query = query.where(TextSimilarityInput.tsp_id > after_tsp_id)
    return list(session.execute(query).scalars().all())


def select_global_ids_for_tsp_ids(session, tsp_ids):
    query = select(SentencesVector.global_id).where(
        SentencesVector.tsp_id.in_(list(tsp_ids))
    )
    return list(session.execute(query).scalars().all())


def select_a_numbers_for_tsp_ids(session, tsp_ids):
    query = (
        select(TextSimilarityInput.a_number)
        .where(TextSimilarityInput.tsp_id.in_(list(tsp_ids)))
        .distinct()
    )
    return set(session.execute(query).scalars().all())


def _first_tsp_id_per_a_number(session, filters):
    rows = session.execute(
        select(TextSimilarityInput.a_number, TextSimilarityInput.tsp_id)
        .where(*filters)
        .order_by(TextSimilarityInput.a_number, TextSimilarityInput.tsp_id)
    ).all()
    first = {}
    for a_number, tsp_id in rows:
        first.setdefault(a_number, tsp_id)
    return list(first.values())


def similar_cases_cleanup(session, a_numbers,):
    a_numbers = list(a_numbers)
    sc_tables = [MicroServicesSimilarCases, SimilarCasesEmb]
    for table in sc_tables:
        affected_a_numbers_list_of_json = (
            session.execute(
                select(table.similar_case_ids).where(table.a_number.in_(a_numbers))
            )
            .scalars()
            .all()
        )
        affected_a_numbers_list = list(
            itertools.chain.from_iterable(
                ids for ids in affected_a_numbers_list_of_json if ids
            )
        )
        affected_a_numbers_set = set(affected_a_numbers_list)
        affected_tsp_ids = _first_tsp_id_per_a_number(
            session,
            [TextSimilarityInput.a_number.in_(list(affected_a_numbers_set))]
        )

        remaining_tsp_ids = _first_tsp_id_per_a_number(
            session,
            [
                TextSimilarityInput.a_number.in_(a_numbers),
                TextSimilarityInput.document_state != "READY_FOR_DELETION",
            ],
        )
        recalculate_case_sim_for_tsp_ids = affected_tsp_ids + remaining_tsp_ids
        logger.info(
            f"Updating {len(recalculate_case_sim_for_tsp_ids)} records to DOC_SIMILARITY_COMPLETE"
        )
        if recalculate_case_sim_for_tsp_ids:
            session.execute(
                update(TextSimilarityInput)
                .where(TextSimilarityInput.tsp_id.in_(recalculate_case_sim_for_tsp_ids))
                .values(document_state="DOC_SIMILARITY_COMPLETE"),
                execution_options=immutabledict({"synchronize_session": "fetch"}),
            )
        deletion = delete(table).where(table.a_number.in_(a_numbers))
        session.execute(
            deletion,
            execution_options=immutabledict({"synchronize_session": "fetch"}),
        )
        session.flush()
        logger.info(f"Finished cleanup of {table.__tablename__} table")


def get_partition_map_using_hash(session, global_ids, modulus=16):
    """
    Maps each global_id to the correct hash partition using PostgreSQL's hashint4().
    """

    sql = text("""
        SELECT global_id,
        ((hashint4extended(global_id, 8816678312871386365)::numeric + 5305509591434766563) % :modulus)::int AS partition
        FROM unnest(:ids) AS global_id
    """)
    result = session.execute(sql, {"ids": global_ids, "modulus": modulus})
    partition_map = {}
    for global_id, partition in result:
        partition = partition if partition >= 0 else (partition + modulus)
        partition_map.setdefault(partition, []).append(global_id)
    return partition_map


def batched(iterable: list[Any], batch_size: int):
    for i in range(0, len(iterable), batch_size):
        yield iterable[i : i + batch_size]


def sentences_vector_table_cleanup(session, global_ids_to_remove, batch_size=10):
    partition_map = get_partition_map_using_hash(session, global_ids_to_remove)
    for partition, g_ids in partition_map.items():
        for batch in batched(g_ids, batch_size):
            logger.info(f"Deleting {len(batch)} from tsp.sentences_vector_p{partition}")
            delete = text(
                f"DELETE from tsp.sentences_vector_p{partition} WHERE global_id = ANY(:g_ids)"
            )
            session.execute(delete, {"g_ids": batch})


def embeddings_table_cleanup(session, global_ids_to_remove, batch_size=10):
    for batch in batched(global_ids_to_remove, batch_size):
        logger.info(f"Deleting {len(batch)} from tsp.embeddings")
        delete_query = text("DELETE FROM tsp.embeddings WHERE global_id = ANY(:g_ids)")
        session.execute(delete_query, {"g_ids": batch})


def matches_table_cleanup(session, global_ids_to_remove):
    # remove from matches jsonb
    _ = update_jsonb(session, global_ids_to_remove)
    # remove the rows for the removed global_ids
    matches_deletion = delete(Matches).where(
        Matches.global_id.in_(global_ids_to_remove),
    )
    session.execute(
        matches_deletion,
        execution_options=immutabledict({"synchronize_session": "fetch"}),
    )
    # remove the rows that now have no matches
    matches_deletion = delete(Matches).where(Matches.match_count == 0)
    session.execute(
        matches_deletion,
        execution_options=immutabledict({"synchronize_session": "fetch"}),
    )


def sentence_matches_table_cleanup(
    session,
    global_ids_to_remove,
    batch_size=150,
):
    sm_tables = [SentenceMatchesTitan]
    for table in sm_tables:
        logger.info(f"Starting batch delete process in {table.__tablename__} table")
        for batch in batched(global_ids_to_remove, batch_size):
            logger.info(f"Found {len(batch)} global_ids to remove")
            global_id_in_source_column = table.source_global_id.in_(batch)
            global_id_in_target_column = table.target_global_id.in_(batch)
            # separated it so it uses an index scan.
            deletion_query = delete(table).where(
                global_id_in_source_column,
            )
            result = session.execute(
                deletion_query,
                execution_options=immutabledict({"synchronize_session": "fetch"}),
            )
            deletion_count = result.rowcount
            deletion_query = delete(table).where(
                global_id_in_target_column,
            )
            result = session.execute(
                deletion_query,
                execution_options=immutabledict({"synchronize_session": "fetch"}),
            )
            deletion_count += result.rowcount
            session.flush()
            logger.info(
                f"Successfully deleted {deletion_count} records in {table.__tablename__}"
            )


def update_jsonb(session, global_ids_to_remove, table="matches", column="matches"):
    # First get the list of global_ids that need updating
    # Find records using ORM
    table_object, find_global_ids_query = _build_query_for_table(table, column)

    # Get all global_ids to update
    result = session.execute(
        find_global_ids_query, {"g_ids": global_ids_to_remove}
    ).fetchall()
    global_ids = [dict(row._mapping).get("global_id") for row in result]
    if not global_ids:
        return 0
    global_ids_count = len(global_ids)
    logger.info(
        f"Found {global_ids_count} global_ids records that relate to this case in {table}"
    )

    total_updated = 0
    batch_size = 500

    # Process in batches of 500
    logger.info("Starting batch update process")
    for i in range(0, global_ids_count, batch_size):
        batch_global_ids = global_ids[i : i + batch_size]

        logger.info(f"Going to retrieve {len(batch_global_ids)} records from {table}")
        records = (
            session.query(table_object)
            .filter(table_object.global_id.in_(batch_global_ids))
            .all()
        )
        logger.info(f"Retrieved {len(records)} records from {table}")
        # Update each record
        update_mappings = _iterate_through_records(
            records, table, column, global_ids_to_remove
        )
        updated_records_count = len(update_mappings)
        logger.info(f"Found {updated_records_count} records to update in tsp.matches")
        if update_mappings:
            session.bulk_update_mappings(table_object, update_mappings)
            session.flush()
            logger.info(
                f"Successfully updated: {updated_records_count} records in tsp.matches"
            )

        total_updated += updated_records_count

    logger.info(
        f"Found {global_ids_count} records that relate to this case. Successfully updated records: "
        + f"{total_updated}, which have exact global_id match in jsonb column in tsp.matches. "
    )
    return total_updated


def _iterate_through_records(records, table, column, global_ids_to_remove):
    remove_set = set(global_ids_to_remove)
    update_mappings = []

    if not remove_set:
        return update_mappings

    for record in records:
        current_matches = getattr(record, column)
        if not current_matches:
            continue

        new_matches = []
        updated = False

        for match in current_matches:
            if match["global_id"] in remove_set:
                updated = True
            else:
                new_matches.append(match)
        if updated:
            setattr(record, column, new_matches)
            to_append = {
                "global_id": record.global_id,
                column: getattr(record, column),
            }
            if table == "matches":
                to_append["match_count"] = len(new_matches)
            update_mappings.append(to_append)
    return update_mappings


def _build_query_for_table(table, column):
    table_object = None
    find_global_ids_query = None
    if table == "matches":
        table_object = Matches
        find_global_ids_query = text(f"""
            WITH match_elements AS (
                SELECT jsonb_array_elements({column}) AS match, global_id
                FROM tsp.{table}
                WHERE global_id = ANY(:g_ids)
            ),
            global_id_matches AS (
                SELECT (match->>'global_id')::bigint AS global_id
                FROM match_elements
            )
            SELECT DISTINCT(global_id) from global_id_matches
            """)
    if table_object is None:
        raise ValueError("The table parameter has a value not supported so far")
    return table_object, find_global_ids_query


def send_to_state_machine_sqs(tsp_ids_for_deletion, sqs_client):
    if len(tsp_ids_for_deletion) == 0:
        return
    logger.info(
        "Sending tsp ids to SQS events in batches",
    )
    tsp_ids_chunked = np.array_split(
        tsp_ids_for_deletion,
        int(np.ceil(len(tsp_ids_for_deletion) / 10))
    )
    for chunk in tsp_ids_chunked:
        entries = [
            json.dumps(
                {"source": "P2", "tsp_id": str(tsp_id), "sent_by": "deletion_job"}
            )
            for tsp_id in chunk
        ]
        sqs_client.send_batch_messages(
            batch_messages=entries, message_group_id=uuid.uuid4().hex
        )


def _atomic_save_faiss_and_dict(mapping_dict_object, faiss_index, mapping_dict_pickle_file_path, faiss_file_path):
    tmp_pickle = f"{mapping_dict_pickle_file_path}"
    tmp_faiss = f"{faiss_file_path}"
    save_faiss_and_dict(
        mapping_dict_object=mapping_dict_object,
        faiss_index=faiss_index,
        mapping_dict_pickle_file_path=tmp_pickle,
        faiss_file_path=tmp_faiss,
    )
    os.replace(tmp_faiss, faiss_file_path)
    os.replace(tmp_pickle, mapping_dict_pickle_file_path)


def index_removal(analysis_group, global_ids_to_remove):
    embedding_type = "word2vec"
    mapping_dict_base, the_index = load_faiss_and_mapping(analysis_group, embedding_type)
    count = the_index.remove_ids(np.array(global_ids_to_remove))
    popped = 0
    for k in global_ids_to_remove:
        if mapping_dict_base.pop(k, None) is not None:
            popped += 1
    logger.info(f"Removed {count!s} global_ids from the {embedding_type} index")

    manage_faiss_path = get_manage_faiss_path()
    _atomic_save_faiss_and_dict(
        mapping_dict_object=mapping_dict_base,
        faiss_index=the_index,
        mapping_dict_pickle_file_path=manage_faiss_path[
            f"{embedding_type.lower()}_{analysis_group.lower()}_mapping_dict_pickle_file"
        ],
        faiss_file_path=manage_faiss_path[
            f"{embedding_type.lower()}_{analysis_group.lower()}_faiss_file"
        ],
    )
    keep_track_of_deletions_to_faiss_index(analysis_group, global_ids_to_remove)


def pause_or_unpause_service(
    db: DatabaseConnection, service="DOC2VEC", pause: bool = True
):
    with db.create_session() as session:
        update_statement = (
            update(MicroServicesStatus)
            .where(MicroServicesStatus.micro_service == service)
            .values(is_paused=pause)
        )
        session.execute(
            update_statement,
            execution_options=immutabledict({"synchronize_session": "fetch"}),
        )
        session.commit()


def load_faiss_and_mapping(analysis_group, embeddings_type):
    manage_faiss_path = get_manage_faiss_path()
    mapping_dict, the_index = load_faiss_and_dict(
        manage_faiss_path[
            f"{embeddings_type.lower()}_{analysis_group.lower()}_mapping_dict_pickle_file"
        ],
        manage_faiss_path[
            f"{embeddings_type.lower()}_{analysis_group.lower()}_faiss_file"
        ],
    )
    return mapping_dict, the_index


def keep_track_of_deletions_to_faiss_index(
    analysis_group, global_ids_to_remove, file_name="tracking_deletions.json"
):
    """
    Function that adds global ids that have been removed from faiss index
    to a json file in S3 to keep track of deletions between faiss backups.

    Args:
        analysis_group (str): refugee or asylee
        global_ids_to_remove (list of int): list of global ids that have been successfully removed
        file_name (str): file name of where to track deletions within faiss index
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_client = boto3.client("s3")
    env = os.getenv("ENVIRONMENT")
    s3_bucket = f"rfs-ta-backup-faiss-files-{env}"

    try:
        file_name_w_analysis_group = analysis_group.lower() + "_" + file_name
        response = s3_client.get_object(
            Bucket=s3_bucket, Key=file_name_w_analysis_group
        )
        existing_content_dict = response["Body"].read().decode("utf-8")
        if existing_content_dict == "":
            existing_content_dict = {timestamp: global_ids_to_remove}
        else:
            existing_content_dict = json.loads(existing_content_dict)
            existing_content_dict[timestamp] = global_ids_to_remove
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=file_name_w_analysis_group,
            Body=json.dumps(existing_content_dict),
            ContentType="application/json",
        )
        logger.info(f"Successfully added global ids {global_ids_to_remove} \
            to {file_name_w_analysis_group} in {s3_bucket}.")
    except Exception as e:  # noqa: BLE001
        logger.error(f"There was an issue {e} reading the {file_name_w_analysis_group} \
            in {s3_bucket} and new deletions can not be tracked.")
