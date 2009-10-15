package org.sodeja.rel;

import java.util.Date;

public class DomainTest {
	public static void main(String[] args) {
		Domain domain = setupDomain();
		domain.insertPlain("Property", 
				"address", "Sofia",
				"price", 120000,
				"photo", null,
				"agent", "agent1",
				"dateRegistered", new Date());
		
		domain.insertPlain("Offer",
				"address", "Sofia",
				"offerPrice", 119000,
				"offerDate", new Date(),
				"bidderName", "bidder1",
				"bidderAddress", "bidder1 add");
		
		domain.insertPlain("Room", 
				"address", "Sofia",
				"roomName", "Main",
				"width", 5.0,
				"breadth", 5.0,
				"type", RoomType.LIVING_ROOM
				);
		
		domain.insertPlain("Floor",
				"address", "Sofia",
				"roomName", "Main",
				"floor", 1);
		
		System.out.println("Property: " + domain.select("Property"));
		System.out.println("Offer: " + domain.select("Offer"));
		System.out.println("Room: " + domain.select("Room"));
		System.out.println("Floor: " + domain.select("Floor"));
		
		System.out.println();
		System.out.println("RoomInfo: " + domain.select("RoomInfo"));
		System.out.println("RoomInfo: " + domain.select("RoomInfo"));
	}

	private static Domain setupDomain() {
		Domain dom = new Domain();
		
		Type address = dom.alias(Types.STRING);
		Type agent = dom.alias(Types.STRING);
		Type name = dom.alias(Types.STRING);
		Type price = dom.alias(Types.DOUBLE);
		Type filename = dom.alias(Types.STRING);
		
		Type roomType = dom.enumType(RoomType.class);
		Type priceBand = dom.enumType(PriceBand.class);
		Type areaCode = dom.enumType(AreaCode.class);
		Type speedBand = dom.enumType(SpeedBand.class);
		
		// Base
		
		dom.relation("Property",
				new Attribute("address", address),
				new Attribute("price", price),
				new Attribute("photo", filename),
				new Attribute("agent", agent),
				new Attribute("dateRegistered", Types.DATE))
				.primaryKey("address");
		
		dom.relation("Offer",
				new Attribute("address", address),
				new Attribute("offerPrice", price),
				new Attribute("offerDate", Types.DATE),
				new Attribute("bidderName", name),
				new Attribute("bidderAddress", address))
				.primaryKey("address", "offerDate", "bidderName", "bidderAddress")
				.foreignKey(dom.resolve("Property"), "address");
		
		dom.relation("Decision",
				new Attribute("address", address),
				new Attribute("offerDate", Types.DATE),
				new Attribute("bidderName", name),
				new Attribute("bidderAddress", address),
				new Attribute("decisionDate", Types.DATE),
				new Attribute("accepted", Types.BOOL))
				.primaryKey("address", "offerDate", "bidderName", "bidderAddress")
				.foreignKey(dom.resolve("Offer"), "address", "offerDate", "bidderName", "bidderAddress");

		dom.relation("Room",
				new Attribute("address", address),
				new Attribute("roomName", Types.STRING),
				new Attribute("width", Types.DOUBLE),
				new Attribute("breadth", Types.DOUBLE),
				new Attribute("type", roomType))
				.primaryKey("address", "roomName")
				.foreignKey(dom.resolve("Property"), "address");

		dom.relation("Floor",
				new Attribute("address", address),
				new Attribute("roomName", Types.STRING),
				new Attribute("floor", Types.INT))
				.primaryKey("address", "roomName")
				.foreignKey(dom.resolve("Property"), "address");
		
		dom.relation("Commission",
				new Attribute("priceBand", priceBand),
				new Attribute("areaCode", areaCode),
				new Attribute("saleSpeed", speedBand),
				new Attribute("commission", Types.DOUBLE))
				.primaryKey("priceBand", "areaCode", "saleSpeed");
		
		// Internal
		
		
		CalculatedAttribute roomSizeAtt = new CalculatedAttribute("roomSize", Types.DOUBLE) {
			@Override
			public Object calculate(Entity entity) {
				double width = (Double) entity.getValue("width");
				double breadth = (Double) entity.getValue("breadth");
				return width * breadth;
			}};
		CalculatedAttribute priceBandAtt = new CalculatedAttribute("priceBand", priceBand) {
			@Override
			public Object calculate(Entity entity) {
				throw new UnsupportedOperationException();
			}};
		CalculatedAttribute areaCodeAtt = new CalculatedAttribute("areaCode", areaCode) {
			@Override
			public Object calculate(Entity entity) {
				throw new UnsupportedOperationException();
			}};
		CalculatedAttribute numberOfRoomsAtt = new CalculatedAttribute("numberOfRooms", Types.INT) {
			@Override
			public Object calculate(Entity entity) {
				throw new UnsupportedOperationException();
			}};
		CalculatedAttribute squareFeetAtt = new CalculatedAttribute("squareFeet", Types.DOUBLE) {
			@Override
			public Object calculate(Entity entity) {
				throw new UnsupportedOperationException();
			}};
		CalculatedAttribute saleSpeedAtt = new CalculatedAttribute("saleSpeed", speedBand) {
			@Override
			public Object calculate(Entity entity) {
				throw new UnsupportedOperationException();
			}};
		
		dom.extend("RoomInfo", "Room", roomSizeAtt);
		
		dom.project_away("Acceptance", dom.restrict("Decision", new Condition() {}), "accepted");
		
		dom.project_away("Rejection", dom.restrict("Decision", new Condition() {}), "accepted");
		
		dom.extend("PropertyInfo", "Property", priceBandAtt, areaCodeAtt, numberOfRoomsAtt, squareFeetAtt);
		
		dom.summarize("CurrentOffer", "Offer", dom.project("Offer", "address", "bidderName", "bidderAddress"), new Aggregate() {});
		
		dom.project_away("RawSales", dom.join("Acceptance", 
				dom.join("CurrentOffer", dom.project("Property", "address", "agent", "dateRegistered"))),
			"offerDate", "bidderName", "bidderAddress");
		
		dom.project("SoldProperty", "RawSales", "address");
		
		dom.minus("UnsoldProperty", dom.project("Property", "address"), "SoldProperty");
		
		dom.project("SalesInfo", dom.extend("RawSales", areaCodeAtt, saleSpeedAtt, priceBandAtt),
			"address", "agent", "areaCode", "saleSpeed", "priceBand");
		
		dom.project("SalesCommissions", dom.join("SalesInfo", "Commission"), "address", "agent", "commission");
		
		// External
		
		dom.join("OpenOffers", "CurrentOffer", dom.minus(
				dom.project_away(dom.resolve("CurrentOffer"), "offerPrice"),
				dom.project_away(dom.resolve("Decision"), "accepted", "decisionDate")));
		
		dom.project("PropertyForWebSite", dom.join("UnsoldProperty", "PropertyInfo"), "address", "price", "photo", "numberOfRooms", "squareFeet");
		
		dom.project("CommissionDue", dom.summarize("SalesCommissions", dom.project("SalesCommissions", "agent"), new Aggregate() {}), "agent", "totalCommission");
		
		return dom;
	}
}
