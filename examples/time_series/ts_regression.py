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
beta = ts_regression(prices, volume, 5, rettype="beta")

print("ts_regression() - Rolling OLS regression")
print("=" * 50)
print("\n5-day rolling beta (price vs volume, partial windows min 2):")
print(beta.head(7))

# Calculate rolling alpha (intercept)
alpha_reg = ts_regression(prices, volume, 5, rettype="alpha")
print("\n5-day rolling alpha (intercept):")
print(alpha_reg.head(7))

# Calculate residual
resid = ts_regression(prices, volume, 5, rettype="resid")
print("\n5-day rolling residual:")
print(resid.head(7))

# Cleanup
client.close()
