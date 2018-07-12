

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * Data Object to store LocalState 
 * for a given SnapShotID
 * */

public class LocalState {

	private int balance = 0;
	private Map<String, ChannelState> channelRecording;

	public LocalState(int currentBalance, List<BranchID> bList) {
		this.balance = currentBalance;
		channelRecording = new HashMap<String, ChannelState>();

		for (BranchID bID : bList) {
			channelRecording.put(bID.name, new ChannelState(true, 0));
		}
	}

	public int getBalance() {
		return balance;
	}

	public void setBalance(int balance) {
		this.balance = balance;
	}

	public Map<String, ChannelState> getChannelRecording() {
		return channelRecording;
	}

	public void setChannelRecording(Map<String, ChannelState> channelRecording) {
		this.channelRecording = channelRecording;
	}

}
