import logging
from zeep import Client, Settings

wsdl_services = {
    "employee_service": "https://api.nmbrs.nl/soap/v3/EmployeeService.asmx?WSDL",
    "company_service": "https://api.nmbrs.nl/soap/v3/CompanyService.asmx?WSDL",
    "debtor_service": "https://api.nmbrs.nl/soap/v3/DebtorService.asmx?WSDL",
}


class Nmbrs:
    def __init__(self, username: str, token: str):
        settings = Settings(extra_http_headers={"Username": username, "Token": token})

        self.employee_client = Client(
            wsdl=wsdl_services.get("employee_service"),
            settings=settings,
        )
        self.company_client = Client(
            wsdl=wsdl_services.get("company_service"),
            settings=settings,
        )

    def get_companies(self):
        """
        method to obtain information of the companies under the domain.

        Returns:
        - List of companies dict containting
            ID (int), number (int), Name (string), PhoneNumber (string), FaxNumber (string), Email (String), Website (string), LoonaangifteTijdvak (), and KvkNr (string)
        """
        logging.info("retreiving information about companies under domain")
        return self.company_client.service.List_GetAll()

    def get_company_employee_ids(self, company_id: int):
        """
        Method to get a list of Employee ids of a company.

        Parameters:
        - company_id (int): ID of the company

        Returns
        - List of employees dict containing
            ID (int), number (int), and displayname (string)
        """
        logging.info("retreiving employee ids from company with id %i" % company_id)
        self.employee_client.service.List_GetByCompany(company_id)

    def get_employee_absence_list(self, employee_id):
        """
        Method to get a list of the absences of an employee based on ID

        Parameters:
        - employee_id (int): ID of the employee

        Returns:
        - List with Absences dict containing
            absenceID (int), comment (string), percentage (int), start (dateTime), RegistrationStartDate (dateTime),
            end (dateTime), registrationEndDate (dateTime), Dossier (string), dossiernr (int)
        """
        self.employee_client.service.Absence_GetList(employee_id)
