package org.sodeja.rel;

import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Set;

import org.sodeja.collections.CollectionUtils;
import org.sodeja.functional.Function1;

public class DomainTest {
	public static void main(String[] args) {
		Domain domain = setupDomain();
		Date offerDate = new Date();
		domain.insertPlain("Property", 
				"address", "Sofia, OK",
				"price", 120000.0,
				"photo", null,
				"agent", "agent1",
				"dateRegistered", offerDate);
		domain.insertPlain("Property", 
				"address", "Sofia, ML",
				"price", 90000.0,
				"photo", null,
				"agent", "agent1",
				"dateRegistered", offerDate);
		
		domain.insertPlain("Offer",
				"address", "Sofia",
				"offerPrice", 119000.0,
				"offerDate", offerDate,
				"bidderName", "bidder1",
				"bidderAddress", "bidder1 add");
		domain.insertPlain("Offer",
				"address", "Sofia",
				"offerPrice", 120000.0,
				"offerDate", new Date(),
				"bidderName", "bidder1",
				"bidderAddress", "bidder1 add");
		domain.insertPlain("Offer",
				"address", "Sofia",
				"offerPrice", 121000.0,
				"offerDate", offerDate,
				"bidderName", "bidder2",
				"bidderAddress", "bidder2 add");
		
		domain.insertPlain("Decision",
				"address", "Sofia",
				"offerDate", offerDate,
				"bidderName", "bidder1",
				"bidderAddress", "bidder1 add",
				"decisionDate", new Date(),
				"accepted", false);
		
		domain.insertPlain("Decision",
				"address", "Sofia",
				"offerDate", offerDate,
				"bidderName", "bidder2",
				"bidderAddress", "bidder2 add",
				"decisionDate", new Date(),
				"accepted", true);
		
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
		System.out.println("Decision: " + domain.select("Decision"));
		System.out.println("Room: " + domain.select("Room"));
		System.out.println("Floor: " + domain.select("Floor"));
		System.out.println();
		
		System.out.println("RoomInfo: " + domain.select("RoomInfo"));
		System.out.println();
		
		System.out.println("Acceptance: " + domain.select("Acceptance"));
		System.out.println("Rejection: " + domain.select("Rejection"));
		System.out.println();
		
		System.out.println("PropertyInfo: " + domain.select("PropertyInfo"));
		System.out.println();
		
		System.out.println("CurrentOffer: " + domain.select("CurrentOffer"));
	}

	private static Domain setupDomain() {
		final Domain dom = new Domain();
		
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
				double price = (Double) entity.getValue("price");
				if(price < 50000) {
					return PriceBand.LOW;
				} else if(price < 75000) {
					return PriceBand.MEDIUM;
				} else if(price < 100000) {
					return PriceBand.HIGH;
				} else {
					return PriceBand.PREMIUM;
				}
			}};
		CalculatedAttribute areaCodeAtt = new CalculatedAttribute("areaCode", areaCode) {
			@Override
			public Object calculate(Entity entity) {
				String address = (String) entity.getValue("address");
				if(address.startsWith("Sofia")) {
					return AreaCode.CITY;
				} else {
					return AreaCode.SUBURBAN;
				}
			}};
		CalculatedAttribute numberOfRoomsAtt = new CalculatedAttribute("numberOfRooms", Types.INT) {
			@Override
			public Object calculate(Entity entity) {
				final String address = (String) entity.getValue("address");
				return dom.restrict("RoomInfo", new Condition() {
					@Override
					public boolean satisfied(Entity e) {
						String iaddress = (String) e.getValue("address");
						return address.equals(iaddress);
					}}).select().size();
			}};
		CalculatedAttribute squareFeetAtt = new CalculatedAttribute("squareFeet", Types.DOUBLE) {
			@Override
			public Object calculate(Entity entity) {
				final String address = (String) entity.getValue("address");
				Set<Entity> result = dom.restrict("RoomInfo", new Condition() {
					@Override
					public boolean satisfied(Entity e) {
						String iaddress = (String) e.getValue("address");
						return address.equals(iaddress);
					}}).select();
				return CollectionUtils.sumDouble(result, new Function1<Double, Entity>() {
					@Override
					public Double execute(Entity p) {
						return (Double) p.getValue("roomSize");
					}});
			}};
		CalculatedAttribute saleSpeedAtt = new CalculatedAttribute("saleSpeed", speedBand) {
			@Override
			public Object calculate(Entity entity) {
				throw new UnsupportedOperationException();
			}};
		
		dom.extend("RoomInfo", "Room", roomSizeAtt);
		
		dom.project_away("Acceptance", dom.restrict("Decision", new Condition() {
			@Override
			public boolean satisfied(Entity e) {
				return (Boolean) e.getValue("accepted");
			}}), "accepted");
		
		dom.project_away("Rejection", dom.restrict("Decision", new Condition() {
			@Override
			public boolean satisfied(Entity e) {
				return ! (Boolean) e.getValue("accepted");
			}}), "accepted");
		
		dom.extend("PropertyInfo", "Property", priceBandAtt, areaCodeAtt, numberOfRoomsAtt, squareFeetAtt);
		
		dom.summarize("CurrentOffer", "Offer", dom.project("Offer", "address", "bidderName", "bidderAddress"), new Aggregate() {
			@Override
			public Entity aggregate(Set<Entity> entities) {
				return Collections.max(entities, new Comparator<Entity>() {
					@Override
					public int compare(Entity o1, Entity o2) {
						Date d1 = (Date) o1.getValue("offerDate");
						Date d2 = (Date) o2.getValue("offerDate");
						return d1.compareTo(d2);
					}});
			}});
		
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
		
		dom.project("CommissionDue", dom.summarize("SalesCommissions", dom.project("SalesCommissions", "agent"), new Aggregate() {
			@Override
			public Entity aggregate(Set<Entity> entities) {
				throw new UnsupportedOperationException();
			}}), "agent", "totalCommission");
		
		return dom;
	}
}
