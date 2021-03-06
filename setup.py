from setuptools import find_packages, setup

setup(
    name="bulky",
    version="1.0.1",
    description="A library with bulk operations on SQLAlchemy and PostgreSQL",
    author="Alexander Sidorov",
    author_email="alex.n.sidorov@gmail.com",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords=" ".join(
        sorted({"bulk", "db", "postgresql" "sqlalchemy", "sqlalchemy-core"})
    ),
    packages=find_packages(
        exclude=("build", "contrib", "dist", "docs", "tests", ".env", "settings.toml")
    ),
    install_requires=(
        "SQLAlchemy>=1.1",
        "psycopg2-binary>=2.7",
        "Jinja2>=2.10.1",
        "typeguard>=2",
    ),
    python_requires=">=3.6, <4",
)
