package BookTopN.BookSecondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Books implements WritableComparable<Books>{
	private String readerId;
	private String bookId;
	private String bookName;
	private String borrowDate;
	private String backDate;
	private String address;

	public void write(DataOutput out) throws IOException {
		//序列化
		out.writeUTF(this.readerId);
		out.writeUTF(this.bookId);
		out.writeUTF(this.bookName);
		out.writeUTF(borrowDate);
		out.writeUTF(this.backDate);
		out.writeUTF(this.address);
	}
	public void readFields(DataInput in) throws IOException {
		//反序列化
		this.readerId=in.readUTF();
		this.bookId=in.readUTF();
		this.bookName=in.readUTF();
		this.borrowDate=in.readUTF();
		this.backDate=in.readUTF();
		this.address=in.readUTF();
	}
	public int compareTo(Books o) {
		// 按照多个属性的排序，先按索引号排序，然后按读者证号排序
		//1 索引号
		int comp=this.bookId.compareTo(o.bookId);//值为相应位上的字符之差
		if (comp!=0){
			return comp;
		}else{//2 如果索引号相同，则按照读者号排序
			return this.readerId.compareTo(o.readerId);
		}
	}

	@Override
	public String toString() {
		return readerId + "\t" + bookId + "\t" + bookName + "\t"
				+ borrowDate + "\t" + backDate + "\t" + address;
	}

	public String getReaderId() {
		return readerId;
	}

	public void setReaderId(String readerId) {
		this.readerId = readerId;
	}

	public String getBookId() {
		return bookId;
	}

	public void setBookId(String bookId) {
		this.bookId = bookId;
	}

	public String getBookName() {
		return bookName;
	}

	public void setBookName(String bookName) {
		this.bookName = bookName;
	}

	public String getBorrowDate() {
		return borrowDate;
	}

	public void setBorrowDate(String borrowDate) {
		this.borrowDate = borrowDate;
	}

	public String getBackDate() {
		return backDate;
	}

	public void setBackDate(String backDate) {
		this.backDate = backDate;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}
}
