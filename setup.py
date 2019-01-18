from setuptools import find_packages, setup

setup(
    name="bulky",
    version="0.0.1b1",
    description="Utilites for bulk operations using SQLAlchemy and PostgreSQL",
    author="Alexander Sidorov",
    author_email="alex.n.sidorov@gmail.com",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.6",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords=" ".join(
        sorted({"bulk", "db", "postgresql" "sqlalchemy", "sqlalchemy-core"})
    ),
    packages=find_packages(exclude=("build", "contrib", "dist", "docs", "tests")),
    install_requires=("SQLAlchemy>=1.1", "psycopg2-binary>=2.7", "jinja2>=2"),
    python_requires=">=3",
)
