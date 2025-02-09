class DecodeService:

    def decrypt_desc_indx_name(self):
        from decode.decrypt_card import decrypt_desc_indx_name

        return decrypt_desc_indx_name()

    def decrypt_ids(self):
        from decode.decrypt_ids import decrypt_ids

        return decrypt_ids()