"""Service for handling card data decryption operations."""

from typing import Any


class DecodeService:
    """Service class for decrypting card data."""

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
