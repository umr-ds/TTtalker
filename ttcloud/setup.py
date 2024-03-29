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
    install_requires=["paho-mqtt", "influxdb", "scikit-learn"],
    extras_require={"lora": ["RPi.GPIO", "spidev"]},
    scripts=[
        "ttt/radio_communication_interface.py",
        "ttt/local_decision_engine.py",
        "ttt/data_archiver.py",
        "ttt/network_coordinator.py",
        "ttt/aggregator.py",
        "ttt/dummy_radio.py",
    ],
    zip_safe=True,
)
