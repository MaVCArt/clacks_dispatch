import setuptools

with open('readme.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name='clacks_dispatch',
    version='0.0.1',
    author='Mattias Van Camp',
    author_email='mavcart.mvc@gmail.com',
    description='Extension library for the clacks framework to provide a dispatch server model.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/MaVCArt/clacks_dispatch',
    packages=setuptools.find_packages(),
    classifiers=[
        'Programming Language :: Python',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)
