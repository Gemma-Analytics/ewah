from setuptools import setup, find_packages

setup(
    name='ewah',
    version='0.1.1',
    description='An ELT with airflow helper module: Ewah',
    url='https://github.com/soltanianalytics/ewah',
    author='Bijan Soltani',
    author_email='me+ewah@bijansoltani.com',
    license='MIT',
    packages=['ewah'],
    zip_safe=False,
    python_requires=">=3.7",
)
