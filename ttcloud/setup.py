from setuptools import setup, find_packages

setup(
    name="treetalkertalker",
    version="0.0.1",
    description="Impersonates a TreeTalker-Cloud",
    author="Markus Sommer",
    author_email="msommer@informatik.uni-marburg.de",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    packages=find_packages(exclude=["tests"]),
    install_requires=["paho-mqtt", "influxdb"],
    extras_require={"lora": ["RPi.GPIO", "spidev"]},
    scripts=["ttt/rci.py", "ttt/lde.py"],
    zip_safe=True,
)
