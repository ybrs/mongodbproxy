from setuptools import setup

setup(
    name='mongo-proxy',
    version='1.0',
    long_description=__doc__,
    packages=['mongoproxy'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[],
    entry_points = {
        'console_scripts': [
            'mongo-proxy = mongoproxy.proxy:run_proxy',
            'mongo-proxy-bench = mongoproxy.bench:main',
        ],
    }
)
