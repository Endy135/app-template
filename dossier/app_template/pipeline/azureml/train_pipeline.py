import os
import joblib
import argparse
from azureml.core import Run
from sklearn import datasets
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn import metrics


def train_random_forest_classifier(model_name, n_estimators, max_depth):
    """Train RandomForestClassifier."""
    # Get the experiment run context
    run = Run.get_context()

    # load the MNIST dataset
    digits = datasets.load_digits()
    X = digits.data
    y = digits.target

    # Split data into training set and test set
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=0
    )

    # Log parameters
    run.log("n_estimators", n_estimators)
    run.log("max_depth", max_depth)

    # Train a RandomForestClassifier
    clf = RandomForestClassifier(
        n_estimators=n_estimators, max_depth=max_depth, random_state=0
    )
    clf.fit(X_train, y_train)

    # Make predictions
    y_train_pred = clf.predict(X_train)
    y_test_pred = clf.predict(X_test)

    # Log metrics
    run.log("train_accuracy", metrics.accuracy_score(y_train, y_train_pred))
    run.log("test_accuracy", metrics.accuracy_score(y_test, y_test_pred))

    # Save the trained model in the output folder
    os.makedirs("output", exist_ok=True)
    joblib.dump(value=clf, filename=f"output/{model_name}")

    # Complete the experiment
    run.complete()


if __name__ == "__main__":
    # Parse the arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-name", type=str, default="model_random_forest")
    parser.add_argument("--n-estimators", type=int, default=10)
    parser.add_argument("--max-depth", type=int, default=10)
    args = parser.parse_args()

    train_random_forest_classifier(
        args.model_name,
        args.n_estimators,
        args.max_depth
    )
