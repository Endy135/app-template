import os
import logging
import argparse
import azureml.core
from azureml.core import Workspace
from azureml.core.compute import ComputeTarget, AmlCompute
from azureml.core.compute_target import ComputeTargetException
from azureml.core import ScriptRunConfig, Experiment, Environment


from app_template.configuration.app import AppConfig


def run_experiment(experiment_name, model_name, n_estimators, max_depth):
    """Run Azure ML experiment."""
    # Load configuration
    app_config = AppConfig()
    azureml_config = app_config.azureml_config

    # Workspace
    ws = Workspace(
        subscription_id=azureml_config["subscription_id"],
        resource_group=azureml_config["resource_group"],
        workspace_name=azureml_config["workspace_name"],
    )
    logging.info(f"Ready to use Azure ML {azureml.core.VERSION} to work with {ws.name}")

    # Create / Load a compute target & verify that the cluster exists
    cluster_name = azureml_config["cluster_name"]
    cluster_vm_size = azureml_config["cluster_vm_size"]
    cluster_max_nodes = azureml_config["cluster_max_nodes"]

    try:
        training_cluster = ComputeTarget(workspace=ws, name=cluster_name)
        logging.info("Found existing cluster, use it")

    except ComputeTargetException:
        # If not, create it
        logging.warning(f"No existing cluster '{cluster_name}', creating it")
        compute_config = AmlCompute.provisioning_configuration(
            vm_size=cluster_vm_size, max_nodes=cluster_max_nodes
        )
        training_cluster = ComputeTarget.create(ws, cluster_name, compute_config)
        logging.info(
            f"Cluster '{cluster_name}' created, size: {cluster_vm_size}, "
            f"max node: {cluster_max_nodes}"
        )

    training_cluster.wait_for_completion(show_output=True)

    # Create a Python environment for the experiment
    env = Environment.from_pip_requirements(
        "env", file_path=os.path.join(os.environ["UNXAPPLI"], "requirements.txt")
    )
    env.python.user_managed_dependencies = False  # Let Azure ML manage dependencies
    env.docker.enabled = True  # Use a docker container

    # Create a ScriptRunConfig
    arguments = [
        "--model-name", model_name,
        "--n-estimators", n_estimators,
        "--max-depth", max_depth,
    ]
    src = ScriptRunConfig(
        source_directory=os.environ["UNXPACKAGE"],
        script="pipeline/azureml/train_pipeline.py",
        arguments=arguments,
        compute_target=cluster_name,
        environment=env
    )

    # Create an experiment
    experiment = Experiment(workspace=ws, name=experiment_name)

    # Run the experiment
    experiment.submit(config=src)
    logging.info("Running the experiment")


if __name__ == "__main__":
    # Parse the arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("experiment_name", type=str,
                        help="Name of the experiment where the job will run")
    parser.add_argument("--model-name", type=str, default="model_random_forest")
    parser.add_argument("--n-estimators", type=int, default=10)
    parser.add_argument("--max-depth", type=int, default=10)
    args = parser.parse_args()

    run_experiment(
        args.experiment_name,
        args.model_name,
        args.n_estimators,
        args.max_depth
    )
