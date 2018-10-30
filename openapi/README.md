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
    Detailled description of this endpoint
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

* Describe the endpoint in the function's docstring in the following format (this will allow the Sphinx documentation to be updated as well):

```
def get_new_endpoint_function(param1):
    """
    Detailled description of this endpoint

    Summary:
        A short summary of what this endpoint does

    Tags:
        program
        
    Args:
        param1 (str): description of the path parameter
        body_input (schema_body_input): description of the body input
    
    Query Args:
        param2 (str): description of the query parameter

    Responses:
        200 (schema_response_200): Success
        403: Unauthorized request
    """
    #################
    # function body #
    #################
```

* A schema can be made reusable by defining it in the file `definitions.py` in Swagger format.

Example:
```
definitions = {
    'schema_response_200': {
        'type': 'object',
        'properties': {
            'object_name': {
                'type': 'string',
            }
        }
    },
    'schema_body_input': { ... }
}
```


* In `sheepdog/blueprint/routes/__init__.py`:
    * If needed, use the `swagger` keyword to overwrite the properties from the docstring or to add more [Swagger properties](https://swagger.io/docs/specification/2-0/basic-structure/) in Swagger format.
    * Note that parameters defined here do not replace parameters defined in the docstring, but are added to them.
    * This is useful, for example, to define different summaries for 2 endpoints PUT and POST pointing to the same method.

Example:

```
new_route(
    '/new_endpoint/<param1>',
    path_to_function.get_new_endpoint_function,
    methods=['GET'],
    swagger={
        'summary': 'An updated summary of what this endpoint does'
    }
)
```

**Note that the current automation is temporary: the way sheepdog handles endpoint registration will be updated and documentation generation will be made easier.**
