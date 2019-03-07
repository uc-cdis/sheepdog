#!/usr/bin/env python

"""
Utility for testing parsing XMLs
"""
import json
from argparse import ArgumentParser

from sheepdog.utils.transforms import BcrClinicalXmlToJsonParser


def parse_args():

    parser = ArgumentParser(description="VCR XML Parser utility")
    parser.add_argument("-m", "--mapping",
                        help="Data Model mapping to use",
                        required=False)

    parser.add_argument("-x", "--xml", help="XML file location to parse", type=str, required=True)
    parser.add_argument("-o", "--output", help="Output filename to dump produced json", type=str)

    return parser.parse_args()


def main():

    args = parse_args()

    parser = BcrClinicalXmlToJsonParser(project_code=None, mapping=args.mapping)
    with open(args.xml, "r+") as xml:
        parser.loads(xml.read())

    if args.output:
        with open(args.output, "w+") as out:
            json.dump(parser.json, out, indent=4, sort_keys=True)
    else:
        print(json.dumps(parser.json, indent=4, sort_keys=True))


if __name__ == "__main__":
    main()
