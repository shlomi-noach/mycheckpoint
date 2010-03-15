from distutils.core import setup

setup(
    name="mycheckpoint",
    description= "Lightweight, SQL oriented monitoring for MySQL",
    platforms= ["Linux", "Un*x", "Python"],
    license= "BSD",
    author="Shlomi Noach",
    author_email="shlomi@code.openark.org",
    url="http://code.openark.org/forge/mycheckpoint",
    version="revision.placeholder",
    requires=["MySQLdb"],
    scripts=[
        "scripts/mycheckpoint",
        ],
    data_files=[
            ("/etc", ["etc/mycheckpoint.cnf"])
        ]
)
