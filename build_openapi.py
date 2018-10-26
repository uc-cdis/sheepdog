import collections
from flasgger import Swagger, Flasgger
from flasgger.utils import swag_from
import yaml
from yaml.representer import Representer

from sheepdog.api import app, app_info
from sheepdog.blueprint.routes import routes


def write_swagger(swagger):
    """
    Generate the Swagger documentation and store it in a file.
    """
    yaml.add_representer(collections.defaultdict, Representer.represent_dict)
    yaml.Dumper.ignore_aliases = lambda *args : True
    outfile = 'openapi/swagger.yml'
    with open(outfile, 'w') as spec_file:
        main_doc = Flasgger.get_apispecs(swagger)

        from openapi.definitions import definitions
        main_doc['definitions'] = definitions

        main_doc = add_routes_swag(main_doc) # add the blueprints' doc
        yaml.dump(main_doc, spec_file, default_flow_style=False)

        print('Generated docs')


def parse_docstring(docstring, path, schema):
    """
    Parse a docstring into its components and translate them into Swagger format.
    """
    result = {}
    if docstring:
        docstring_list = [line.strip() for line in docstring.splitlines() if line]

        # description: first block of text in the docstring
        if docstring_list[0].startswith('/'):
            start = 2 # the first 2 lines sometimes describe the path and method
        else:
            start = 0
        i = start
        while i < len(docstring_list) \
            and not docstring_list[i].startswith(':') \
            and not docstring_list[i].startswith('Args:'):
            i += 1
        description = ' '.join(docstring_list[start:i])
        result['description'] = description

        parameters = []
        # parameters listed under 'Args'
        if i < len(docstring_list) and docstring_list[i].startswith('Args:'):
            i += 1
            while i < len(docstring_list) \
            and not docstring_list[i].startswith(':'):
                parts = docstring_list[i].split(' ')
                if len(parts) < 3: # we're now out of Args list
                    break
                param_name = parts[0]
                param_desc = ' '.join(parts[2:]).replace('|', '')
                parameters.append({
                    'name': param_name,
                    'in': 'path' if (param_name in path) else 'query',
                    'required': True if (param_name in path) else False,
                    'type': 'string',
                    'description': param_desc
                })
                i += 1

        responses = {}
        for line in docstring_list:

            # parameters listed as 'param' (path parameters)
            if line.startswith(':param'):
                parts = line.split(' ')
                param_name = parts[2].replace(':', '')
                param_desc = ' '.join(parts[3:]).replace('|', '')
                parameters.append({
                    'name': param_name,
                    'in': 'path' if (param_name in path) else 'query',
                    'required': True if (param_name in path) else False,
                    'type': 'string',
                    'description': param_desc
                })

            # parameters listed as 'query' (optional parameter)
            if line.startswith(':query'):
                parts = line.split(' ')
                param_name = parts[1].replace(':', '')
                param_desc = ' '.join(parts[2:]).replace('|', '')
                parameters.append({
                    'name': param_name,
                    'in': 'query',
                    'type': 'string',
                    'description': param_desc
                })

            # responses listed as 'statuscode'
            if line.startswith(':statuscode'):
                parts = line.split(' ')
                status_code = parts[1].replace(':', '')
                desc = ' '.join(parts[2:])
                responses[status_code] = {
                    'description': desc
                }
                # schema of the body for this response
                if schema and status_code in schema:
                    ref = '#/definitions/{}'.format(schema[status_code])
                    responses[status_code]['schema'] = {
                        '$ref': ref
                    }

        # description of the input body
        if schema and 'body' in schema:
            ref = '#/definitions/{}'.format(schema['body'])
            parameters.append({
                'name': 'body',
                'in': 'body',
                'schema': {
                    '$ref': ref
                }
            })

        if parameters:
            result['parameters'] = parameters
        result['responses'] = responses

    return result


def add_routes_swag(main_doc):
    """
    Read the Swagger doc for each blueprint and add it to a main doc.
    """
    for route in routes:

        spec = {}
        docstring = route['view_func'].__doc__
        spec = parse_docstring(docstring, route['rule'], route['schema'])

        if not spec:
            print('This function has no doc: ' + route['view_func'].__name__)

        # overwrite parsed info with manually written 'swagger' field info
        # note: parameters are added, not overwritten
        new_spec = route['swagger'] if route['swagger'] else {}
        new_params = new_spec.pop('parameters', [])
        if new_params:
            if 'parameters' in spec:
                spec['parameters'].extend(new_params)
            else:
                spec['parameters'] = new_params
        spec.update(new_spec)

        # methods: GET, PUT, POST, DELETE
        for method in route['options'].get('methods', []):
            # path = '/v0/submission' + route['rule']
            path = route['rule']
            if path not in main_doc['paths']:
                main_doc['paths'][path] = {}
            
            main_doc['paths'][path][method.lower()] = spec

    return main_doc


if __name__ == '__main__':
    with app.app_context():
        swagger = Swagger(app, template=app_info)
        write_swagger(swagger)
