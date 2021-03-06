package util;



import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.Socket;

/**
 *
 * @author Ryan
 */
public class TCPCLientController {
    private Socket socket;
    private ObjectInputStream ois;
    private ObjectOutputStream oos;
    public TCPCLientController(InetAddress IP, int port){
        System.out.println("Clien TCP with"+port+ "is running");
        try{
            socket = new Socket(IP, port);
            System.out.println("Socket: "+IP.getHostAddress()+":"+port);
            getStream();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public void getStream(){
        try{
            ois = new ObjectInputStream(socket.getInputStream());
            oos = new ObjectOutputStream(socket.getOutputStream());
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public String readData() throws IOException, ClassNotFoundException {
        return (String) ois.readObject();
    }


}
