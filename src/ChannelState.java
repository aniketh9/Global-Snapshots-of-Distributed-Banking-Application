

/*
 * Data Object to store channel state for a
 * given SnapShotID and a LocalState
 * */
public class ChannelState {

	private Boolean recordState;
	private int recievedMoney;

	public ChannelState(Boolean recordState, int recievedMoney) {
		super();
		this.recordState = recordState;
		this.recievedMoney = recievedMoney;
	}

	public Boolean getRecordState() {
		return recordState;
	}

	public void setRecordState(Boolean recordState) {
		this.recordState = recordState;
	}

	public int getRecievedMoney() {
		return recievedMoney;
	}

	public void setRecievedMoney(int recievedMoney) {
		this.recievedMoney = recievedMoney;
	}

}
