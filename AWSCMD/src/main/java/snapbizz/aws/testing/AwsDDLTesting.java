package snapbizz.aws.testing;

import snapbizz.aws.service.DdlOperationService;
import snapbizz.aws.serviceImpl.DdlOperationServiceImpl;

public class AwsDDLTesting {

	public static void main(String[] args) {
		DdlOperationService ddlOperation = new DdlOperationServiceImpl();
		//ddlOperation.getAllTablesName();
		//ddlOperation.getTableInformation("test_products");

	}

}
