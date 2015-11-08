package DDS.team22.GeospatialOperations;

import java.sql.Timestamp;

public class Utils {

	public static Float[] splitingStringToFloat(String inputData,String Separator){
        String[] SplitString=inputData.split(Separator); 
        Float[] fnew= new Float[SplitString.length];
        for(int i=0;i<SplitString.length;i++){
            fnew[i]=Float.parseFloat(SplitString[i]);
        }
           return fnew;
    }
	
	public static Long getCurrentTime() {
		return System.currentTimeMillis();
	}
	
}
