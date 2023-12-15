"""Module to send notification after job execution."""
import os


def send_email(mailer, type_execution, kpis_table_name=None, execution_kpis=None, exception=None):
    """Send an email after the the pipeline runs.

    Parameters
    ----------
    mailer : connected_vehicle_standard.configuration.mails.Mailer
        Mailer object.
    type_execution : str, either "success" or "error"
        Whether the execution was successful or not.
    kpis_table_name : str, mandatory if type_execution='success'
        Oracle SQL table name.
    execution_kpis : dict, mandatory if type_execution='success'
        Execution KPIs.
    exception : str, mandatory if type_execution='error'
        Exception raised by the application.

    """
    if type_execution == "success":
        mess_kpis = "   - " + "<br>   - ".join([f"{k}: {v}" for k, v in execution_kpis.items()])
        mailer.send(
            subject="[app00] Successful production KPIs processing",
            message=(
                f"""Production KPIs pipeline successfully executed.
                     <br>Production KPIs are written in the Oracle table {kpis_table_name.upper()}
                     <br><br><u><b>KPIs overview</u>:</b>
                     <br>{mess_kpis}
                     """
            ),
            links={"Site Stellantis": "https://www.stellantis.com/fr"},
        )

    elif type_execution == "error":
        mailer.send(
            "[app00] Error production KPIs processing",
            (
                f"""An exception was raised during the execution of the production KPIs pipeline:
                <br><br>{exception}
                <br><br>Please check the logs at the location {os.environ['UNXLOG']}
                """
            ),
        )
