

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class DBranchHandler implements Branch.Iface {

	private volatile List<BranchID> listOfBranches = new ArrayList<BranchID>();
	private static volatile int branchBalance = 0;
	private static volatile int initialBranchBalance = 0;
	private volatile BranchID localBranchID = new BranchID();
	private static volatile Map<String, Integer> sendingMsgIDMap = new ConcurrentHashMap<String, Integer>();
	private volatile Map<String, Integer> receievingMsgIdMap = new ConcurrentHashMap<String, Integer>();
	private volatile Map<Integer, LocalState> snapShotMap = new ConcurrentHashMap<Integer, LocalState>();
	private volatile List<BranchID> preserveBranchOrder;

	public void initBranch(int balance, List<BranchID> all_branches) throws SystemException, TException {

		DBranchHandler.branchBalance = balance;
		DBranchHandler.initialBranchBalance = balance;
		preserveBranchOrder = all_branches;

		for (int i = 0; i < all_branches.size(); i++) {
			if (!all_branches.get(i).name.equals(localBranchID.name)) {
				listOfBranches.add(all_branches.get(i));
				sendingMsgIDMap.put(all_branches.get(i).name, 0);
				receievingMsgIdMap.put(all_branches.get(i).name, 0);
			}
		}

	}

	public void transferMoney(TransferMessage message, int messageId) throws SystemException, TException {

		while (receievingMsgIdMap.get(message.orig_branchId.name).intValue() + 1 != messageId) {

			/*
			 * Condition while loop which waits on incoming message ID to be
			 * (currentMessagId + 1) for a given branch so that FIFO message
			 * delivery is maintained
			 */
		}

/*		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}*/

		for (Map.Entry<Integer, LocalState> entry : snapShotMap.entrySet()) {

			LocalState localState1 = entry.getValue();

			if (localState1.getChannelRecording().get(message.orig_branchId.name).getRecordState()) {

				Map<String, ChannelState> channelRecording1 = localState1.getChannelRecording();
				ChannelState channelState1 = channelRecording1.get(message.orig_branchId.name);
				channelState1.setRecievedMoney(channelState1.getRecievedMoney() + message.amount);
				channelRecording1.replace(message.orig_branchId.name, channelState1);
				localState1.setChannelRecording(channelRecording1);
				snapShotMap.replace(entry.getKey(), localState1);
			}

		}
		receievingMsgIdMap.replace(message.orig_branchId.name,
				receievingMsgIdMap.get(message.orig_branchId.name).intValue() + 1);

		DBranchHandler.branchBalance += message.amount;

	}

	public void initSnapshot(int snapshotId) throws SystemException, TException {

		LocalState recordState = new LocalState(this.branchBalance, preserveBranchOrder);
		snapShotMap.put(snapshotId, recordState);
		this.sendMarkers(snapshotId, listOfBranches);
	}

	public void Marker(BranchID branchId, int snapshotId, int messageId) throws SystemException, TException {

		while (receievingMsgIdMap.get(branchId.name).intValue() + 1 != messageId) {

			/*
			 * Condition while loop which waits on incoming message ID to be
			 * equal to (currentMessagId + 1) for a given branch so that FIFO
			 * message delivery is maintained
			 */
		}

		LocalState recordState;

		if (!snapShotMap.containsKey(snapshotId)) {

			recordState = new LocalState(DBranchHandler.branchBalance, preserveBranchOrder);
			Map<String, ChannelState> channelRecording = recordState.getChannelRecording();
			ChannelState channelState = channelRecording.get(branchId.name);
			channelState.setRecordState(false);
			channelRecording.replace(branchId.name, channelState);
			recordState.setChannelRecording(channelRecording);
			snapShotMap.put(snapshotId, recordState);
			this.sendMarkers(snapshotId, listOfBranches);

		} else {

			recordState = snapShotMap.get(snapshotId);
			Map<String, ChannelState> channelRecording = recordState.getChannelRecording();
			ChannelState channelState = channelRecording.get(branchId.name);
			channelState.setRecordState(false);
			channelRecording.replace(branchId.name, channelState);
			recordState.setChannelRecording(channelRecording);
			snapShotMap.replace(snapshotId, recordState);

		}
		receievingMsgIdMap.replace(branchId.name, receievingMsgIdMap.get(branchId.name).intValue() + 1);

	}

	public LocalSnapshot retrieveSnapshot(int snapshotId) throws SystemException, TException {

		LocalState localState2 = snapShotMap.get(snapshotId);
		List<Integer> channelList = new ArrayList<Integer>();

		for (Map.Entry<String, ChannelState> entry : localState2.getChannelRecording().entrySet()) {

			channelList.add(entry.getValue().getRecievedMoney());
		}
		LocalSnapshot localSnap = new LocalSnapshot(snapshotId, localState2.getBalance(), channelList);
		snapShotMap.remove(snapshotId);
		return localSnap;
	}

	public void sendMarkers(int snapshotId, List<BranchID> lst) {
		/*
		 * Grabbing the messageID per branch so that while sending markers no
		 * transfer money is sent between.
		 */
		List<Integer> getMessageIds = new ArrayList<Integer>(lst.size());
		int startIndex = 0;

		for (BranchID branch1 : lst) {
			int updateMsgID = sendingMsgIDMap.get(branch1.name) + 1;
			sendingMsgIDMap.replace(branch1.name, updateMsgID);
			getMessageIds.add(updateMsgID);
		}

		for (BranchID branch : lst) {

			try {
				TTransport transport = new TSocket(branch.ip, branch.port);
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				Branch.Client client = new Branch.Client(protocol);
				client.Marker(localBranchID, snapshotId, getMessageIds.get(startIndex));
				transport.close();
				startIndex++;
			} catch (TException e) {
				e.printStackTrace();
				System.exit(1);

			}
		}
	}

	public static int getInitialBranchBalance() {
		return initialBranchBalance;
	}

	public Map<String, Integer> getMessageIdMap() {
		return sendingMsgIDMap;
	}

	public int getBranchBalance() {
		return branchBalance;
	}

	public void setBranchBalance(int branchBalance) {
		DBranchHandler.branchBalance = branchBalance;
	}

	public List<BranchID> getListOfBranches() {
		return listOfBranches;
	}

	public BranchID getBranchID() {
		return localBranchID;
	}

	public void setBranchID(BranchID branchID) {
		this.localBranchID = branchID;
	}

}
