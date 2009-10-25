from distutils.core import setup

setup(
    name="mycheckpoint",
    description="Monitoring for MySQL",
    author="Shlomi Noach",
    author_email="shlomi@code.openark.org",
    url="http://code.openark.org/forge/mycheckpoint",
    version="18",
    requires=["MySQLdb"],
    packages=[""],
    package_dir={"": "scripts"},
    scripts=[
        "mycheckpoint",
        ]
)
