# Trade Trail

This repo is a supporting service for a trading platform that combines multiple modules for portfolio allocation, auto-trading, messaging, testing, sentiment analysis, and securities clustering. The platform includes a "game" feature where users can compete against each other and trading bots in a simulated trading session implemented via websockets.


## Modules
### Portfolio Allocation
The portfolio allocation module automates user investments by calculating the optimal allocation of assets based on a user's risk tolerance, financial goals, and market conditions. This module is designed to help users achieve their investment objectives while minimizing risks.
### Trading
The trading module includes bots for auto trading and competitions against other users. The bots are trained using historical data and advanced machine learning algorithms to make intelligent trading decisions. The competitions allow users to test their skills against each other and against the bots.
### Sentiment Analysis
The sentiment analysis module includes a news and Twitter pipeline that calculates the sentiment for specific securities. This module is designed to help users make more informed trading decisions by providing real-time market sentiment data.
### Clustering
The clustering module is responsible for securities clustering, which groups similar securities together based on various factors. This module is designed to help users identify potential investment opportunities and minimize risks.
### Producers
The producers module contains scripts to send messages to our Go app with RabbitMQ. This module is responsible for delivering real-time data to the platform and ensuring that users have access to the latest market information.
### Testing
The testing module includes a comprehensive suite of tests to ensure the platform functions as expected. This module is designed to catch any bugs or issues before they affect users.
### Trading Session
The platform includes a "game" feature that allows users to compete against each other and trading bots in a simulated trading session. The winner is the user with the most profits at the end of the session. The game logic is implemented using FastAPI websockets, and the app.py file contains a sentiment GET endpoint to get sentiment, news, and tweets for a specific ticker.

## Deploymnent
The company logos are saved in a Firebase database, news and tweets are saved in MongoDB cloud, and RabbitMQ is deployed on Amazon MQ. The platform is deployed using Docker on Railway, which is a platform as a service (PaaS) that makes it easy to deploy and manage containers.
## Technologies Used
- Python
- Go
- Swift
- FastAPI
- Websockets
- RabbitMQ
- MongoDB
- Firebase
- Docker
- Railway
## Installation
To install the platform, follow these steps:

1. Clone the repository: `git clone https://github.com/Chinedu-E/TradeTrail-py.git`
2. Install the dependencies: `pip install -r requirements.txt`
3. Run the app: `uvicorn src.app:app`
## Conclusion
This trading platform is a comprehensive solution for users who want to automate their investments, trade against other users and bots, and analyze market sentiment. With its advanced modules and game feature, the platform is designed to provide a fun and engaging trading experience while helping users achieve their investment goals.