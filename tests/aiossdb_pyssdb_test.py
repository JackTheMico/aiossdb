import logging
import pytest
import hashlib

from pyssdb import Client as pyClient

from aiossdb import Client

# 和size有关的命令返回值不一样，pyssdb返回的是int,aiossdb返回的都是bytes;
# zget返回的权重值也不一样，同上

# 以上两点已通过修改源码解决

# 另外aiossdb之所以返回值统一都是bytesArray，是因为用了asyncio这个库，
# 其中的StreamReader, StreamWriter（reader, writer）文档中写的读和写返回的都是bytes。


@pytest.mark.asyncio
async def test_aiossdb_kv(event_loop):
    """
    """
    logger = logging.getLogger("aiossdb_kv")
    aio_cli = Client(host="0.0.0.0", port=38888, loop=event_loop)
    await aio_cli.set("test_kv", 1234567890)
    res = await aio_cli.get("test_kv")
    logger.debug(res)
    await aio_cli.close()
    assert res == b"1234567890"


def test_pyssdb_kv():
    """

    """
    logger = logging.getLogger("pyssdb_kv")
    py_cli = pyClient(host="0.0.0.0", port=38888)
    py_cli.set("test_kv", 1234567890)
    res = py_cli.get("test_kv")
    logger.debug(res)
    assert res == b"1234567890"


@pytest.mark.asyncio
async def test_aiossdb_keys(event_loop):
    """

    :Keyword Arguments:
     event_loop --
    :return: None
    """
    aio_cli = Client(host="0.0.0.0", port=38888, loop=event_loop)
    res = await aio_cli.keys("A", "F", 5)
    assert res == [b'B', b'C', b'D', b'E', b'F']
    await aio_cli.close()


def test_pyssdb_keys():
    """

    :Keyword Arguments:
    :return: None
    """
    py_cli = pyClient(host="0.0.0.0", port=38888)
    res = py_cli.keys("A", "F", 5)
    assert res == [b'B', b'C', b'D', b'E', b'F']


@pytest.mark.asyncio
async def test_aiossdb_queue(event_loop):
    """
    Keyword Arguments:
    event_loop --
    """
    logger = logging.getLogger("aiossdb_queue")
    aio_cli = Client(host="0.0.0.0", port=38888, loop=event_loop)
    await aio_cli.qpush_front("aiossdb_queue", 123)
    await aio_cli.qpush_back("aiossdb_queue", 456)
    qsize = await aio_cli.qsize("aiossdb_queue")
    assert qsize == 2
    res = await aio_cli.qpop_front("aiossdb_queue", 1)
    logger.debug(res)
    assert res == b"123"
    await aio_cli.qpop_front("aiossdb_queue", 1)
    await aio_cli.close()


def test_pyssdb_queue():
    """

    """
    logger = logging.getLogger("pyssdb_kv")
    py_cli = pyClient(host="0.0.0.0", port=38888)
    py_cli.qpush_front("aiossdb_queue", 123)
    qsize = py_cli.qsize("aiossdb_queue")
    assert qsize == 1
    res = py_cli.qpop_front("aiossdb_queue")
    logger.warning(res)
    assert res == b"123"


@pytest.mark.asyncio
async def test_aiossdb_queue_mult(event_loop):
    """
    Keyword Arguments:
    event_loop --
    """
    logger = logging.getLogger("aiossdb_queue")
    aio_cli = Client(host="0.0.0.0", port=38888, loop=event_loop)
    await aio_cli.qpush_front("aiossdb_queue", 123, 456, 789)
    res = await aio_cli.qpop_front("aiossdb_queue", 3)
    logger.debug(res)
    await aio_cli.close()
    assert res[0].decode() == "789"


def test_pyssdb_queue_mult():
    """

    """
    logger = logging.getLogger("pyssdb_kv")
    py_cli = pyClient(host="0.0.0.0", port=38888)
    py_cli.qpush_front("aiossdb_queue", 123, 456, 789)
    res = py_cli.qpop_front("aiossdb_queue", 3)
    logger.warning(res)
    assert res[0].decode() == "789"


@pytest.mark.asyncio
async def test_aiossdb_zset(event_loop):
    """
    Keyword Arguments:
    event_loop --
    """
    logger = logging.getLogger("aiossdb_zset")
    aio_cli = Client(host="0.0.0.0", port=38888, loop=event_loop)
    await aio_cli.zset("aiossdb_zset", "aiossdb_test", 123)
    res = await aio_cli.zget("aiossdb_zset", "aiossdb_test")
    logger.debug(res)
    await aio_cli.close()
    assert res == 123


def test_pyssdb_zset():
    """

    """
    logger = logging.getLogger("pyssdb_kv")
    py_cli = pyClient(host="0.0.0.0", port=38888)
    py_cli.zset("aiossdb_zset", "aiossdb_test", 123)
    res = py_cli.zget("aiossdb_zset", "aiossdb_test")
    logger.debug(res)
    assert res == 123


@pytest.mark.asyncio
async def test_aiossdb_multi_zset(event_loop):
    """

    :Keyword Arguments:
    event_loop --
    :return: None
    """
    logger = logging.getLogger("aiossdb_multi_zset")
    aio_cli = Client(host="0.0.0.0", port=38888, loop=event_loop)
    test_list = ['key1', 1, 'key2', 2]
    await aio_cli.multi_zset("aiossdb_multi", *test_list)
    get_list = ['key1', 'key2']
    res = await aio_cli.multi_zget("aiossdb_multi", *get_list)
    assert res == [b'key1', b'1', b'key2', b'2']
    zsize = await aio_cli.zsize("aiossdb_multi")
    assert zsize == 2
    await aio_cli.close()


def test_pyssdb_multi_zset():
    """

    :return: None
    """
    logger = logging.getLogger("pyssdb_kv")
    py_cli = pyClient(host="0.0.0.0", port=38888)
    test_list = ['key1', 1, 'key2', 2]
    get_list = ['key1', 'key2']
    py_cli.multi_zset("aiossdb_multi", *test_list)
    res = py_cli.multi_zget("aiossdb_multi", *get_list)
    assert res == [b'key1', b'1', b'key2', b'2']
    zsize = py_cli.zsize("aiossdb_multi")
    assert zsize == 2


@pytest.mark.asyncio
async def test_aiossdb_hash(event_loop):
    """
    Keyword Arguments:
    event_loop --
    """
    logger = logging.getLogger("aiossdb_hash")
    aio_cli = Client(host="0.0.0.0", port=38888, loop=event_loop)
    await aio_cli.hset("aiossdb_hash", "aiossdb_test", 123)
    res = await aio_cli.hget("aiossdb_hash", "aiossdb_test")
    logger.debug(res)
    assert res == b"123"
    await aio_cli.hset("aiossdb_hash", "aiossdb_test1", 123)
    res1 = await aio_cli.hgetall("aiossdb_hash")
    assert res1 == [b'aiossdb_test', b'123', b'aiossdb_test1', b'123']
    hsize = await aio_cli.hsize("aiossdb_hash")
    assert hsize == 2
    await aio_cli.close()


def test_pyssdb_hash():
    """

    """
    logger = logging.getLogger("pyssdb_kv")
    py_cli = pyClient(host="0.0.0.0", port=38888)
    py_cli.hset("aiossdb_hash", "aiossdb_test", 123)
    res = py_cli.hget("aiossdb_hash", "aiossdb_test")
    logger.debug(res)
    assert res == b"123"
    py_cli.hset("aiossdb_hash", "aiossdb_test1", 123)
    res1 = py_cli.hgetall("aiossdb_hash")
    assert res1 == [b'aiossdb_test', b'123', b'aiossdb_test1', b'123']
    hsize = py_cli.hsize("aiossdb_hash")
    assert hsize == 2


@pytest.mark.asyncio
async def test_aiossdb_hashmd5(event_loop):
    """

    :Keyword Arguments:
     event_loop --
    :return: None
    """
    aio_cli = Client(host="0.0.0.0", port=38888, loop=event_loop)
    src = "pretend_to_be_function"
    m1 = hashlib.md5()
    m1.update(src.encode())
    md5_str = m1.hexdigest()
    await aio_cli.hset(md5_str+"%$#@_xxx", md5_str+"%$#@_xxx"+"123", 123)
    res = await aio_cli.hget(md5_str+"%$#@_xxx", md5_str+"%$#@_xxx"+"123")
    assert res == b'123'
    await aio_cli.close()


@pytest.mark.asyncio
async def test_aiossdb_kv_bytes(event_loop):
    """

    :Keyword Arguments:
     event_loop --
    :return: None
    """
    aio_cli = Client(host="192.168.81.60", port=10302, loop=event_loop)
    # await aio_cli.set("test_for_bytes", b"ehehasdfasgasd2134125asdg$#$#./,.2")
    # res = await aio_cli.get("test_for_bytes")
    # assert res == b'ehehasdfasgasd2134125asdg$#$#./,.2'
    res = await aio_cli.get("Cache|taskGroupLogic_7aed37c144e024adab2752b2c931ced1")
    assert res != b''
    await aio_cli.close()
