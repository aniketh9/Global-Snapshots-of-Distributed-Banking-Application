

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Random;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;

public class DistributedBranch {

	public static int branchPort;
	public static volatile DBranchHandler branchHandler;
	public static Branch.Processor processor;

	/*
	 * Main method that spawns two separate threads, 1 for serving the incoming
	 * requests and 2 for sending the money randomly.
	 **/

	public static void main(String[] args) {

		if (args.length != 2) {
			System.out.println("Invalid No of Parameters,please provide valid Parameters");
		}

		try {
			branchHandler = new DBranchHandler();
			branchHandler.getBranchID().setName(args[0]);
			branchPort = Integer.valueOf(args[1]);
			branchHandler.getBranchID()
					.setIp(InetAddress.getByName(InetAddress.getLocalHost().getHostAddress()).toString().substring(1));
			branchHandler.getBranchID().setPort(branchPort);
			processor = new Branch.Processor(branchHandler);

			Runnable serverThread = new Runnable() {
				public void run() {
					receiver(processor, branchHandler.getBranchID().getName());
				}
			};
			new Thread(serverThread).start();

			Runnable clientThread = new Runnable() {
				public void run() {
					sender(branchHandler);
				}
			};
			new Thread(clientThread).start();

		} catch (Exception e) {

			System.out.println("Exception while starting sender and reciever" + e);
			e.printStackTrace();
			System.exit(1);
		}
	}

	/*
	 * Method for receiving requests like TransferMoney ,Marker, InitSnapShot
	 * and Retrieve Snapshots
	 */

	public static void receiver(Branch.Processor processor, String Bname) {
		try {

			TServerTransport serverTransport = new TServerSocket(branchPort);
			TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

			System.out.println("Starting " + Bname + " on port : " + branchPort);
			server.serve();
		} catch (Exception e) {

			System.out.println("Exception inside Receiver thread " + e);
			e.printStackTrace();
			System.exit(1);
		}
	}

	/*
	 * Method that randomly selects a branch and sends money to it, Money is
	 * sent with time gap between [0-5] seconds and amounts-to-be-sent ranges
	 * between [1-5] percentages.
	 */

	public static void sender(DBranchHandler dbHandler) {

		while (true) {

			if (dbHandler.getListOfBranches().size() != 0) {
				int initialBranchBalance = dbHandler.getInitialBranchBalance();
				Random randomGenerator = new Random();
				int percentage = randomGenerator.nextInt(5) + 1;
				int waitTime = randomGenerator.nextInt(6);
				int randomIndex = randomGenerator.nextInt(dbHandler.getListOfBranches().size());
				BranchID branchId = dbHandler.getListOfBranches().get(randomIndex);
				try {

					Thread.sleep(1000 * (waitTime));
					TTransport transport = new TSocket(branchId.ip, branchId.port);
					transport.open();
					TProtocol protocol = new TBinaryProtocol(transport);
					Branch.Client senderClient = new Branch.Client(protocol);
					int branchBalance = dbHandler.getBranchBalance();
					if (branchBalance > 0) {
						int amountTobeSent = (int) (percentage * initialBranchBalance / 100);
						TransferMessage tMessage = new TransferMessage(dbHandler.getBranchID(), amountTobeSent);
						branchBalance = branchBalance - amountTobeSent;
						dbHandler.setBranchBalance(branchBalance);
						int msgIdToSend = dbHandler.getMessageIdMap().get(branchId.name);
						dbHandler.getMessageIdMap().replace(branchId.name, msgIdToSend + 1);
						senderClient.transferMoney(tMessage, msgIdToSend + 1);

					}
					transport.close();

				} catch (InterruptedException e) {
					System.out.println("InterruptedException inside Sender thread" + e);
					e.printStackTrace();
					System.exit(1);
				} catch (TTransportException e) {
					System.out.println("TTransportException inside Sender thread" + e);
					e.printStackTrace();
					System.exit(1);
				} catch (SystemException e) {
					System.out.println("SystemException inside Sender thread" + e);
					e.printStackTrace();
					System.exit(1);
				} catch (TException e) {
					System.out.println("TException inside Sender thread" + e);
					e.printStackTrace();
					System.exit(1);
				}

			}

		}
	}
}