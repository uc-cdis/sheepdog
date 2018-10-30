import collections
import yaml
from yaml.representer import Representer
import re

from openapi.docstring_parsing import Docstring
from sheepdog.blueprint.routes import routes


# TODO:
# request headers
# add health check and version endpoints


def write_swagger(swag_doc):
    """
    Write the Swagger documentation in a file.
    """
    yaml.add_representer(collections.defaultdict, Representer.represent_dict)
    yaml.Dumper.ignore_aliases = lambda *args : True
    outfile = 'openapi/swagger.yml'
    with open(outfile, 'w') as spec_file:
        yaml.dump(swag_doc, spec_file, default_flow_style=False)
        print('Generated docs')


def translate_to_swag(doc):
    """
    Converts a parsed docstring in a dict to a Swagger formatted dict.
    """
    summary = doc['Summary'][0].description if doc.get('Summary') else ''
    spec = {
        'description': doc.get('Description', ''),
        'summary': summary,
        'tags': map(lambda i: i.description, doc.get('Tags', []))
    }

    # Sphinx uses the shortened version of type names -> translate them
    swagger_types = {
        'str': 'string',
        'bool': 'boolean',
        'int': 'integer'
    }

    # Responses and status codes
    resps = doc.get('Responses')
    spec['responses'] = {}
    for code, props in resps.iteritems():
        spec['responses'][code] = {
            'description': props.description
        }
        if props.type:
            ref = '#/definitions/{}'.format(props.type)
            spec['responses'][code]['schema'] = {
                '$ref': ref
            }

    args = doc.get('Args')

    # Path parameters
    spec['parameters'] = [
        {
            'in': 'path',
            'name': name,
            'type': swagger_types.get(props.type, props.type),
            'description': props.description,
            'required': True
        }
        for name, props in args.iteritems()
        if props.name != 'body'
    ]

    # Body input
    spec['parameters'].extend([
        {
            'in': 'body',
            'name': name,
            'description': props.description,
            'schema': {
                '$ref': '#/definitions/{}'.format(props.type)
            }
        }
        for name, props in args.iteritems()
        if props.name == 'body'
    ])

    # Query parameters
    args = doc.get('Query Args')
    spec['parameters'].extend([
        {
            'in': 'query',
            'name': name,
            'type': swagger_types.get(props.type, props.type),
            'description': props.description
        }
        for name, props in args.iteritems()
    ])

    subs = parse_sphinx_substitutions()
    for p in spec['parameters']:
        for k, v in subs.iteritems():
            look_for = '|{}|'.format(k)
            p['description'] = p['description'].replace(look_for, v)

    return spec


def parse_sphinx_substitutions():
    file_name = '../sheepdog/docs/api_reference/substitutions.rst'
    regex = re.compile(r"\|(.*)\|")
    subs = {}
    try:
        with open(file_name, 'r') as f:
            lines = map(lambda i: i.strip(), f.readlines())
            indexes = [i for i, s in enumerate(lines) if 'replace::' in s]
            for i in range(len(indexes)):
                start = indexes[i]
                end = indexes[i + 1] if i < len(indexes) - 1 else len(lines)
                name = regex.findall(lines[start])[0]
                description = ' '.join(lines[start + 1 : end])
                subs[name] = description.strip()
    except IOError:
        print('Substitution file {} not found'.format(file_name))
    return subs


def build_swag_doc():
    """
    Return a compilation of the Swagger docs of all the blueprints.
    """
    from openapi.definitions import definitions
    from openapi.app_info import app_info

    swag_doc = app_info.copy()
    swag_doc['definitions'] = definitions
    swag_doc['paths'] = {}

    # Parse each blueprint's docstring
    for route in routes:

        docstring = route['view_func'].__doc__
        if not docstring:
            print('This endpoint is not documented: {}'.format(route['view_func'].__name__))

        parsed_doc = Docstring.from_string(docstring)
        spec = translate_to_swag(parsed_doc.sections)
        
        # overwrite parsed info with manually written 'swagger' field info
        # (e.g. a PUT and a POST point to the same function but one is for creation and the other for update -> overwritte summary)
        # (e.g. to add a dry_run tag)
        # note: parameters are added, not overwritten
        if route['swagger']:
            new_params = route['swagger'].pop('parameters', [])
            spec['parameters'].extend(new_params)
            spec.update(route['swagger'])

        path = route['rule'] # OR? '/v0/submission' + route['rule']

        # methods: GET, PUT, POST, DELETE
        for method in route['options'].get('methods', []):
            if path not in swag_doc['paths']:
                swag_doc['paths'][path] = {}
            swag_doc['paths'][path][method.lower()] = spec

    return swag_doc


if __name__ == '__main__':
    swag_doc = build_swag_doc()
    write_swagger(swag_doc)