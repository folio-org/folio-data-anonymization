import random

from faker.providers import BaseProvider
from faker import Faker

faker = Faker()


class Organizations(BaseProvider):

    def edi_type(self):
        fake_lib_edi_type = []
        for _ in range(2):
            fake_lib_edi_type.append(str(faker.random_digit()))
        fake_lib_edi_type.append(faker.random_uppercase_letter())
        fake_lib_edi_type.append(f"/{faker.country_code()}-")
        for _ in range(3):
            fake_lib_edi_type.append(faker.random_uppercase_letter())
        return "".join(fake_lib_edi_type)

    def org_code(self):
        fake_org_code = []
        size = random.randint(3, 15)
        for i in range(size):
            if size > 5 and i == size - 4:
                fake_org_code.append("-")
                continue
            fake_org_code.append(faker.random_uppercase_letter())
        return "".join(fake_org_code)


class Users(BaseProvider):

    def pronouns(self):
        return "".join(
            faker.words(
                nb=1, ext_word_list=['she/her/hers', 'he/him/his', 'they/them/theirs']
            )
        )

    def birthdate(self):
        return faker.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d')

    def barcode(self):
        fake_barcode = []
        for _ in range(10):
            fake_barcode.append(str(faker.random_digit()))

        return "".join(fake_barcode)

    def external_system_id(self):
        fake_external_system_id = []
        for _ in range(9):
            fake_external_system_id.append(str(faker.random_digit()))

        return "".join(fake_external_system_id)
