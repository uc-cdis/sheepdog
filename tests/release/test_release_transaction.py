from sheepdog.transactions.release import ReleaseTransaction


def test_perform_release(index_client, pg_driver):
    """
    Args:
        # nodes(list[Node]):
        index_client (indexclient.client.IndexClient):
        pg_driver (psqlgraph.PsqlGraphDriver):
    """
    print(nodes)
    rtxn = ReleaseTransaction(indexd=index_client, db_driver=pg_driver)
    # released = rtxn.perform_release(nodes)

    assert 2 == 3
