import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin


def drop_columns(X, columns):
    X_copy = X.copy()
    return X_copy.drop(columns=columns, errors="ignore")


class MathTransformations(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.column_names = None

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X_copy = X.copy()

        X_copy["errorBalanceOrig"] = X_copy.newbalanceOrig + X_copy.amount - X_copy.oldbalanceOrg
        X_copy["errorBalanceDest"] = X_copy.oldbalanceDest + X_copy.amount - X_copy.newbalanceDest

        X_copy["is_account_empty"] = (X_copy.newbalanceOrig <= 0.01).astype(np.int8)
        X_copy["is_error_destiny_suspicious"] = (X_copy.errorBalanceDest > 10_000).astype(np.int8)

        self.column_names = X_copy.columns
        return X_copy

    def get_feature_names_out(self, input_features=None):
        if self.column_names is None:
            raise ValueError("The transformer has not been fitted...")
        return np.asarray(self.column_names)
