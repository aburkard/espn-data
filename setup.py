from setuptools import setup, find_packages

setup(
    name="espn-data",
    version="0.1.0",
    description="Scraper for ESPN women's college basketball data",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "requests>=2.31.0",
        "pandas>=2.1.1",
        "tqdm>=4.66.1",
        "python-dotenv>=1.0.0",
        "aiohttp>=3.8.5",
        "asyncio>=3.4.3",
    ],
    entry_points={
        "console_scripts": ["espn-scraper=espn_data.__main__:main",],
    },
    python_requires=">=3.8",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)
