from setuptools import setup, find_packages

setup(
    name="merge_json",
    version="0.1",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "Click",
        "boto3",
        "retrying"
    ],
    entry_points="""
        [console_scripts]
        merge_json=merge_json.merge_json:cli
    """,
)