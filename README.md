# Web3 Sustainability Assessment Platform (W3SAP)

Welcome to the **Web3 Sustainability Assessment Platform (W3SAP)** repository! This project is being developed as part of a hackathon to create an AI-driven tool for evaluating and monitoring the sustainability of Ethereum-based projects. W3SAP automates the assessment of sustainability claims by leveraging multiple data sources, APIs, and advanced data processing techniques. The goal is to provide actionable insights that help users make informed decisions regarding the sustainability of Web3 and blockchain projects.

## Project Overview

**W3SAP** aims to assess sustainability metrics for Ethereum-based Web3 projects, providing a robust platform that uses automated tools to verify claims related to energy consumption, carbon offsets, governance, transparency, and overall sustainability. The platform integrates a variety of data sources, APIs, and AI-powered analytics to evaluate and score the sustainability of these projects.

## Pipeline Overview

![Pipeline](asset.png)

Our data pipeline consists of the following key components:

### 1. **Data Collection**
   - **Web Source Service**: Integration with multiple public APIs, such as the **GitHub API**, **DefiLama API**, **Reddit API**, and others to gather relevant data on Web3 projects.
   - **URL Resource Services**: Usage of **Google Search API** and custom-built web scraping tools to discover new URLs and ensure comprehensive data collection.

### 2. **Data Processing and Analysis**
   - **Airflow**: Orchestrates workflows, ensuring efficient data collection and processing.
   - **Data Scoring and Analysis**: A set of engineered formulas to evaluate and score Web3 projects based on visibility in the open-source community and their Web3-related impact.
   - **Neo4j Integration**: A **multi-agent system** utilizes **Neo4j** to connect and aggregate data, providing an accurate representation of project sustainability metrics.

### 3. **User Feedback and Visualization**
   - After data collection and processing, the results will be available for users to review through an intuitive interface that provides a comprehensive sustainability score for each project.

## Future Work

While the foundational work for W3SAP has been established, several areas remain for future improvement:
- **Enhanced Data Integration**: We plan to integrate additional data sources for a more comprehensive assessment.
- **Advanced Analysis Tools**: Further development of analysis modules using more sophisticated AI techniques to improve prediction accuracy and decision-making.
- **User Interface Development**: Building a user-friendly interface for easy navigation, data visualization, and real-time interaction with the sustainability scores.

## Team

- **Hackathon Team**: A group of passionate developers and data scientists working on W3SAP.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
