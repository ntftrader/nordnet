import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="nordnet",
    version="0.1.1",
    author="NTF Trader",
    author_email="ntftrader@gmail.com",
    description="A wrapper for nordnet.no public apis",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ntftrader/nordnet",
    packages=setuptools.find_packages(),
    install_requires=['requests', 'simplejson', 'pandas'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
