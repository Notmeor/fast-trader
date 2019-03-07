
import hashlib
import pickle
import lz4.block


def compress(b):
    return lz4.block.compress(b, mode='fast')


def decompress(b):
    return lz4.block.decompress(b)


class Serializer:

    @staticmethod
    def serialize(obj):
        if isinstance(obj, bytes):
            ret = obj
        elif isinstance(obj, str):
            ret = obj.encode()
        else:
            ret = pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)
        return ret

    @staticmethod
    def deserialize(b):
        if not isinstance(b, bytes):
            return b
        try:
            ret = b.decode()
        except UnicodeDecodeError:
            try:
                ret = pickle.loads(b)
            except pickle.UnpicklingError:
                ret = b
        return ret

    @classmethod
    def gen_md5(cls, b, value=False):
        bytes_ = cls.serialize(b)
        md5 = hashlib.md5(bytes_).hexdigest()
        if value:
            return md5, bytes_
        return md5

serializer = Serializer