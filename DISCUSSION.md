# yearn Vault ROI Calculator Model

## General Idea
This model is meant to be a bayesian estimation for the future value of the PricePerShare of a yearn vault 
based on the past price development.

The data for this are the historic hourly changes in the PricePerShare Value of a vault. These are taken as 
observations and are then sampled with [PyMC3](https://docs.pymc.io/) to get a posterior distribution of
future price developments.
Since the environment of DeFi and yearn Vaults is a highly dynamic one there are different model versions for
each Vault based on the last (7, 30, 90, 365) days.

The price developments are then projected hourly into the future on a linear basis.
For each Vault and each "based on"-period the model runs 10,000 simulations and calculates HPD intervals
from the results. For better readability and display in the frontend the simulation results 
are then resampled to a daily time series.

The model is updated nightly where the prices for the last day are pulled. The the simulations of the future
based on the new informations are done and updated in the webapp database.
 
Find the complete process [here](model/job/tasks/create_model.py)

## Pros
* The model does nicely take into account uncertainty based on the length of the "based on"-period
* It is a simple and easy to understand model that does not make many assumptions 
* The model provides a clear and easy to interpret output

## Cons
* As a general rule the past does not dictate the future. This is especially true for the turbulent world of DeFi
* The price changes are modelled as a normal distribution. 
[A lot has been said](https://en.wikipedia.org/wiki/Black_swan_theory) about how the normal distribution is not an ideal tool for financial modelling.
 
## Data Sources
* [Public Ethereum Big Query Dataset](https://github.com/blockchain-etl/ethereum-etl)
* [archivenode.io](https://archivenode.io/)
* [etherscan.io](https://etherscan.io/)
