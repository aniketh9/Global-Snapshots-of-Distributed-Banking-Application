

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class Controller {

	private static int totalBalance = 0;
	public static List<BranchID> branchList;
	public static volatile int snapShotId = 1;

	/*
	 * Main method that spawns two separate threads, for initiating and
	 * retrieving snapshots
	 **/

	public static void main(String args[]) {
		if (args.length != 2) {
			System.out.println("Invalid No of Parameters,please provide valid Parameters");
			System.exit(1);
		}
		totalBalance = Integer.parseInt(args[0]);
		branchList = getBranchList(args[1]);
		initBranches(branchList, totalBalance);

		Runnable initThread = new Runnable() {
			public void run() {
				initSnapShot(branchList);
			}
		};
		new Thread(initThread).start();
		Runnable retrieveThread = new Runnable() {
			public void run() {
				retrieveSnapShot(branchList);
			}
		};
		new Thread(retrieveThread).start();

	}

	/*
	 * Method to read an input file containing list of Distributed branches ,IP
	 * addresses and their port numbers
	 */

	public static List<BranchID> getBranchList(String fileName) {

		Scanner scanner = null;
		BranchID branchId;
		File inputFile = new File(fileName);
		List<BranchID> bList = new ArrayList<BranchID>();

		try {
			scanner = new Scanner(inputFile);

			while (scanner.hasNextLine()) {

				String branchRecord = scanner.nextLine();
				branchId = new BranchID();
				String[] splitRecord = branchRecord.split("\\s+");
				branchId.name = splitRecord[0];
				branchId.ip = splitRecord[1];
				branchId.port = Integer.parseInt(splitRecord[2]);
				bList.add(branchId);
			}

		} catch (FileNotFoundException e) {
			System.out.println("File " + fileName + " not found");
			e.printStackTrace();
		} finally {
			scanner.close();
		}
		return bList;

	}

	/*
	 * Method to initial each Distributed Branch by passing list of all branches
	 * in the system and allocated balance
	 */

	public static void initBranches(List<BranchID> bList, int balance) {

		int branchBalance = 0;

		if (bList.size() > 0) {
			branchBalance = balance / bList.size();
		}

		for (int i = 0; i < bList.size(); i++) {
			try {
				TTransport transport = new TSocket(bList.get(i).ip, bList.get(i).port);
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				Branch.Client client = new Branch.Client(protocol);
				System.out
						.println("Sent Branch-List and Initial-Balance:" + branchBalance + " to " + bList.get(i).name);
				client.initBranch(branchBalance, branchList);
				transport.close();
			} catch (TException e) {
				System.out.println("TException while Initiating Branches");
				e.printStackTrace();
				System.exit(1);

			}
		}
	}

	/*
	 * Method to trigger initSnapshot method on randomly chosen Distributed
	 * Branch
	 */

	public static void initSnapShot(List<BranchID> bList) {

		while (true) {

			Random randomGenerator = new Random();
			int randomIndex = randomGenerator.nextInt(bList.size());
			int waitTime = randomGenerator.nextInt(5) + 1;

			try {

				Thread.sleep(1000 * waitTime);
				TTransport transport = new TSocket(bList.get(randomIndex).ip, bList.get(randomIndex).port);
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				Branch.Client initClient = new Branch.Client(protocol);
				initClient.initSnapshot(snapShotId);
				snapShotId++;
				transport.close();

			} catch (InterruptedException e) {
				System.out.println("InterruptedException inside initSnapShot thread" + e);
				e.printStackTrace();
				System.exit(1);
			} catch (TException e) {
				System.out.println("TException inside initSnapShot thread" + e);
				e.printStackTrace();
				System.exit(1);
			}

		}

	}

	/*
	 * Method to retrieve Local Snapshots from all the branches in a Distributed
	 * Bank
	 */

	public static void retrieveSnapShot(List<BranchID> bList) {

		int sID = 1;

		while (true) {

			int totalBankBalance = 0;
			int totalChannelBalance = 0;
			int currentIndex = 0;
			try {
				if (sID < snapShotId)
					;
				Thread.sleep(20000);
				System.out.println("GLOBAL SNAPSHOT FOR ID:: " + sID + "\n");
				for (BranchID branchID : bList) {
					TTransport transport = new TSocket(branchID.ip, branchID.port);
					transport.open();
					TProtocol protocol = new TBinaryProtocol(transport);
					Branch.Client retrieveClient = new Branch.Client(protocol);
					LocalSnapshot localSnap = retrieveClient.retrieveSnapshot(sID);
					totalBankBalance += localSnap.balance;
					System.out.print(branchID.name + " balance: " + localSnap.balance + "\t");
					List<Integer> Messages = localSnap.messages;
					int aggregrateChannelMoney = 0;
					for (int k = 0; k < Messages.size(); k++) {

						if (currentIndex != k) {
							System.out.print(bList.get(k).name + "->" + branchID.name + "::" + Messages.get(k) + "\t");
							aggregrateChannelMoney += Messages.get(k);
						}
					}
					totalChannelBalance += aggregrateChannelMoney;
					transport.close();
					currentIndex++;
					System.out.println("\n");
				}
				System.out.println("Total Channel Money that is in transit: " + totalChannelBalance);
				System.out.println("Aggregrated  Branch Balance: " + " is: " + totalBankBalance + "\n");
				sID++;

			} catch (InterruptedException e) {
				System.out.println("InterruptedException inside retrieveSnapShot thread" + e);
				e.printStackTrace();
				System.exit(1);
			} catch (TException e) {
				System.out.println("TException inside retrieveSnapShot thread" + e);
				System.exit(1);
				e.printStackTrace();
			}

		}

	}

}
