import re


class Argument(object):
    """
    Parse a docstring argument from the format:

        name (type): description
    """

    re_arg = re.compile(r"([\w_]+)(?: \(([\w_]+)\))?: ([\w\s_.,'\"`()|]+)")
    #                     ^ name        ^ type        ^ description

    def __init__(self, name = None, arg_type = None, description = None):
        self.name = name
        self.type = arg_type
        self.description = description

    def __str__(self):
        if not self.type:
            return '{name}: {description}'.format(**self.__dict__)
        return '{name} ({type}): {description}'.format(**self.__dict__)

    @classmethod
    def from_string(cls, raw):
        """
        Read an Argument from a line in the docstring.
        """
        raw = raw.strip()
        try:
            [match] = cls.re_arg.findall(raw)
            name, arg_type, description = match
        except ValueError:
            return cls(description = raw) # argument without name or type
        return cls(name, arg_type, description)


class Docstring(object):
    """
    Store docstring arguments by section.
    """

    section_names = ["Args", "Query Args", "Responses", "Summary", "Tags"]
    single_arg_section_names = ["Summary", "Tags"]

    def __init__(self):
        self.sections = {}

    @classmethod
    def parse_description(cls, raw):
        lines = raw.split("\n")
        i = 0
        while i < len(lines) \
            and not lines[i].startswith(tuple(cls.section_names)) \
            and not lines[i].startswith(':'):
            i += 1
        description = ' '.join(filter(None, lines[:i]))
        return description.strip()

    @classmethod
    def parse_section(cls, section, docstring):
        """
        Read an individual section of the docstring (like `Args:`) into a dictionary
        mapping argument names to the Argument objects.
        """
        regex = re.compile(r"^\s*{}:([\s\S]*?)(?:^$|$)\n\n".format(section), re.MULTILINE)
        section_args = ""
        try:
            [section_args] = regex.findall(docstring)
        except ValueError:
            pass

        args = map(Argument.from_string, filter(bool, section_args.split("\n")))
        if section in cls.single_arg_section_names:
            return args
        else:
            return {
                arg.name: arg
                for arg in args
            }

    @classmethod
    def from_string(cls, raw):
        """
        Load a Docstring from the raw string representation.
        """
        raw = "\n".join(line.strip() for line in raw.strip().split("\n")) + "\n\n"
        doc = cls()
        doc.sections['Description'] = cls.parse_description(raw)
        for section_name in cls.section_names:
            doc.sections[section_name] = cls.parse_section(section_name, raw)
        return doc


if __name__ == '__main__':
    example = (
        """
        Some explanation.

        Args:
            a (str): asdfjasdf
            b (bool): asdjfjasdfkfei
            c: no type

        Query Args:
            foo (string): asjdfjskadfj

        Responses:
            200: OK
            400: bad
        """
    )

    doc = Docstring.from_string(example)
    print(doc.sections["Args"]["a"])
    print(doc.sections["Query Args"]["foo"])
    print(doc.sections["Responses"]["200"])
