"""
ts_regression - Rolling OLS regression (y ~ x)

When to use:
    Use ts_regression() to compute rolling betas, alphas, or residuals.
    Essential for factor exposure analysis and hedging.

Parameters:
    y: Dependent variable DataFrame
    x: Independent variable DataFrame
    d: Window size (number of periods)
    rettype: What to return - "beta", "alpha", "resid", "r_squared"

Example output:
    Rolling regression of prices on volume
"""
from dotenv import load_dotenv
load_dotenv()

from quantdl import QuantDLClient
from quantdl.operators import ts_regression

# Initialize client
client = QuantDLClient()

# Fetch price and volume data
symbols = ["IBM", "TXN", "NOW", "BMY", "LMT"]
prices = client.ticks(symbols, field="close", start="2024-01-01", end="2024-06-30")
volume = client.ticks(symbols, field="volume", start="2024-01-01", end="2024-06-30")

# Calculate rolling beta (slope)
beta = ts_regression(prices, volume, 20, rettype="beta")

print("ts_regression() - Rolling OLS regression")
print("=" * 50)
print("\n20-day rolling beta (price vs volume):")
print(beta.tail(5))

# Calculate rolling alpha (intercept)
alpha_reg = ts_regression(prices, volume, 20, rettype="alpha")
print("\n20-day rolling alpha (intercept):")
print(alpha_reg.tail(5))

# Calculate residual
resid = ts_regression(prices, volume, 20, rettype="resid")
print("\n20-day rolling residual (last value):")
print(resid.tail(5))

# Cleanup
client.close()
