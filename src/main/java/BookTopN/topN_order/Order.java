package BookTopN.topN_order;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Order implements WritableComparable<Order>{
	private String orderId;
	private String userId;
	private String product;
	private float price;
	private int number;
	private float sum;
	
	//无参构造方法
	public Order() {
		// TODO Auto-generated constructor stub
	}
	
	//有残构造方法
	public Order(String orderId, String userId, String product, float price, int number, float sum) {
		super();
		this.orderId = orderId;
		this.userId = userId;
		this.product = product;
		this.price = price;
		this.number = number;
		this.sum = sum;
	}


	//完成序列化和反序列化
	//反序列化
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.orderId=in.readUTF();
		this.userId=in.readUTF();
		this.product=in.readUTF();
		this.price=in.readFloat();
		this.number=in.readInt();
		this.sum=in.readFloat();
		
	}

	//序列化
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(this.orderId);
		out.writeUTF(this.userId);
		out.writeUTF(this.product);
		out.writeFloat(this.price);
		out.writeInt(this.number);
		out.writeFloat(this.sum);
	}

	public int compareTo(Order o) {
		// TODO Auto-generated method stub
		return (int)(o.sum-this.sum);
	}

	@Override
	public String toString() {
		return "Order [orderId=" + orderId + ", userId=" + userId + ", product=" + product + ", price=" + price
				+ ", number=" + number + ", sum=" + sum + "]";
	}

	public String getOrderId() {
		return orderId;
	}

	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getProduct() {
		return product;
	}

	public void setProduct(String product) {
		this.product = product;
	}

	public float getPrice() {
		return price;
	}

	public void setPrice(float price) {
		this.price = price;
	}

	public int getNumber() {
		return number;
	}

	public void setNumber(int number) {
		this.number = number;
	}

	public float getSum() {
		return sum;
	}

	public void setSum(float sum) {
		this.sum = sum;
	}
	
	
	
}
