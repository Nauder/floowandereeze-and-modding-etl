"""Service for handling card data decryption operations."""

from typing import Any
import zlib

import msgpack


class DecodeService:
    """Service class for decrypting card data."""

    @staticmethod
    def decode_item_names(script: bytes) -> dict:
        """Decode the game's YDLZ-compressed MessagePack item name table."""
        if script.startswith(b"YDLZ"):
            script = zlib.decompress(script[8:])
        names = msgpack.unpackb(script, raw=False)
        if not isinstance(names, dict):
            raise ValueError("Expected an item name dictionary")
        return {key: value for key, value in names.items() if not key.startswith("_")}

    def decrypt_desc_indx_name(self) -> Any:
        """Decrypt the description index name data.

        Returns:
            Decrypted description index name data.
        """
        from decode.decrypt_card import decrypt_desc_indx_name

        return decrypt_desc_indx_name()

    def decrypt_ids(self) -> Any:
        """Decrypt the card IDs data.

        Returns:
            Decrypted card IDs data.
        """
        from decode.decrypt_ids import decrypt_ids

        return decrypt_ids()
