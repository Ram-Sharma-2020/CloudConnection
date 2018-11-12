package snapbizz.aws.testing;

import java.util.HashMap;
import java.util.Map;

import com.snapbizz.api.models.APIObjects;

import snapbizz.aws.service.DmlOperationService;
import snapbizz.aws.serviceImpl.DmlOperationServiceImpl;

public class AwsDMLTesting {

	public static void main(String[] args) {
		DmlOperationService dmlOperationService = new DmlOperationServiceImpl();
		
		Map<String, Object> parameter = new HashMap();
		parameter.put("hashKey", Integer.valueOf("10002"));
		parameter.put("rangeKey", "code_size");
		parameter.put("rangeKeyValue", "-1");
		
		Map<String, Object> attributeKeyParameter = new HashMap<String, Object>();
		attributeKeyParameter.put("distributor_phone", Long.valueOf("9999999999"));
		
		/*parameter.put("hashKey", Integer.valueOf("10366"));
		parameter.put("rangeKey", "code_size" );
		parameter.put("rangeKeyValue", "-1");*/
		System.out.println("********* STARTED ****************");
		//dmlOperationService.getBatchItems("product_packs", parameter);
		dmlOperationService.getItemByField("product_packs", parameter, "product_code");
		//dmlOperationService.getItem(APIObjects.PRODUCT_PACKS_TABLE, parameter);
		
		//dmlOperationService.getItem(APIObjects.CUSTOMER_MONTHLY_SUMMARY_TABLE, parameter, attributeKeyParameter); //APIObjects.CUSTOMER_DETAIL_TABLE
		System.out.println("********* COMPLETED ****************");
	}

}
