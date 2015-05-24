package edu.sjsu.cmpe.cache.client;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");
        CRDTClient crdtClient = new CRDTClient();
        
        System.out.println("Putting Values:");
        if(crdtClient.put(1, "a")) {
        	System.out.println("put(1 => a)"  + "successfull" );
        }else {
        	System.out.println("put(1 => a)"  + "unsuccessfull" );
        }
        
        Thread.sleep(30000);
        if(crdtClient.put(1, "b")) {
        	System.out.println("put(1 => b)" + "successfull" );
        }else {
        	System.out.println("put(1 => b)"  + "unsuccessfull" );
        }
        
        Thread.sleep(30000);
        
        System.out.println("Reading Values:");
        
        String value = crdtClient.get(1);
        System.out.println("get(1) => " + value);

        System.out.println("Exiting Cache Client...");
    }

}
