# OpenAPI spec

This is the [OpenAPI](https://github.com/OAI/OpenAPI-Specification)/[Swagger 2.0](https://swagger.io/) specification of Sheepdog's REST API, which can be visualized [here](http://petstore.swagger.io/?url=https://raw.githubusercontent.com/uc-cdis/sheepdog/master/openapi/swagger.yml).

The specification in `swagger.yml` is generated in part using a custom script, in part using [Flasgger](https://github.com/rochacbruno/flasgger).

The documentation for blueprint endpoints being [set up using Sphinx](https://github.com/uc-cdis/sheepdog#sphinx) via docstrings, we use a script to parse these docstrings and convert the information to Swagger format.

# To generate the documentation

* in the main sheepdog folder, run `python build_openapi.py`;
* validate the updated `swagger.yml` using the [Swagger editor](http://editor.swagger.io);
* git push `swagger.yml`.

# To add or update documentation for an endpoint

**If this is a Flask endpoint:**

* Update the docstring in Flasgger format.

Example:
```
@app.route('/new_endpoint/<param1>')
def get_new_endpoint_function():
    """
    Description of this endpoint
    ---
    parameters:
      - name: param1
        description: a path parameter
        in: path
        type: string
        required: true
    responses:
      200:
        description: Success
      default:
        description: Something went wrong
    """
    #################
    # function body #
    #################
```

**If this is a blueprint endpoint:**

Note: if new, the endpoint should have been added to `sheepdog/blueprint/routes/__init__.py`.

* Add a description, path parameters, query parameters and status codes as needed by updating the function's docstring in Sphinx format. This will allow the Sphinx documentation to be updated as well.

Example:
```
def get_new_endpoint_function(param1):
    """
    Description of this endpoint
    
    Args:
        param1 (str): a path parameter
    ### OR
    :param str param1: a path parameter
    :query query_param1: a query parameter
    :statuscode 200: Success
    :statuscode default: Something went wrong
    """
    #################
    # function body #
    #################
```
* In `sheepdog/blueprint/routes/__init__.py`:
    * Use the `swagger` keyword to add more [Swagger properties](https://swagger.io/docs/specification/2-0/basic-structure/), such as `summary` or `tags`.
    * Note that if a property is defined both here and in the docstring, the value specified here is used. An exception to this rule is `parameters`: all parameters defined either here or in the docstring will be registered.
    * Use the `schema` keyword to describe the format of an input (key `body`) or an output (key = status code). The schema is to be defined in `openapi/definitions.py`.

Example:

In `__init__.py`:
```
new_route(
    '/new_endpoint/<param1>',
    path_to_function.get_new_endpoint_function,
    methods=['GET'],
    swagger={
        'summary': 'A short summary of what this endpoint does',
        'parameters': [{
            'name': 'query_param2',
            'in' : 'query',
            'description': 'a new query parameter',
            'type': 'string'
        }],
    },
    schema={
        'body': 'schema_input_data' # description of input body
        '200': 'schema_output_data' # description of response when status code is 200
    }
)
```
In `definitions.py`:
```
definitions = {
    'schema_input_data': {
        'type': 'object',
        'properties': {
            'name': {
                'type': 'string',
            }
        }
    },
    'schema_output_data': { ... }
}
```
