from setuptools import setup


setup(
    name='sheepdog',
    version='0.1.0',
    description='Flask blueprint for herding data submissions',
    url='https://github.com/uc-cdis/sheepdog',
    license='Apache',
    packages=[
        'sheepdog',
        'sheepdog.auth',
        'sheepdog.blueprint',
        'sheepdog.blueprint.routes',
        'sheepdog.blueprint.routes.views',
        'sheepdog.blueprint.routes.views.program',
        'sheepdog.transactions',
        'sheepdog.transactions.close',
        'sheepdog.transactions.deletion',
        'sheepdog.transactions.release',
        'sheepdog.transactions.review',
        'sheepdog.transactions.submission',
        'sheepdog.transactions.upload',
        'sheepdog.utils',
        'sheepdog.utils.transforms',
    ],
)
