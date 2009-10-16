package org.sodeja.rel;

import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Set;
import java.util.TreeSet;

import org.sodeja.collections.CollectionUtils;
import org.sodeja.functional.Function1;

public class DomainTest {
	public static void main(String[] args) {
		Domain domain = setupDomain();
		
		domain.begin();
		
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
				"address", "Sofia, OK",
				"offerPrice", 119000.0,
				"offerDate", offerDate,
				"bidderName", "bidder1",
				"bidderAddress", "bidder1 add");
		domain.insertPlain("Offer",
				"address", "Sofia, OK",
				"offerPrice", 120000.0,
				"offerDate", new Date(System.currentTimeMillis() - 10000),
				"bidderName", "bidder1",
				"bidderAddress", "bidder1 add");
		domain.insertPlain("Offer",
				"address", "Sofia, OK",
				"offerPrice", 121000.0,
				"offerDate", offerDate,
				"bidderName", "bidder2",
				"bidderAddress", "bidder2 add");
		domain.insertPlain("Offer",
				"address", "Sofia, ML",
				"offerPrice", 89000.0,
				"offerDate", offerDate,
				"bidderName", "bidder2",
				"bidderAddress", "bidder2 add");
		
		domain.insertPlain("Decision",
				"address", "Sofia, OK",
				"offerDate", offerDate,
				"bidderName", "bidder1",
				"bidderAddress", "bidder1 add",
				"decisionDate", new Date(),
				"accepted", false);
		
		domain.insertPlain("Decision",
				"address", "Sofia, OK",
				"offerDate", offerDate,
				"bidderName", "bidder2",
				"bidderAddress", "bidder2 add",
				"decisionDate", new Date(),
				"accepted", true);
		
		domain.insertPlain("Room", 
				"address", "Sofia, OK",
				"roomName", "Main",
				"width", 5.0,
				"breadth", 5.0,
				"type", RoomType.LIVING_ROOM
				);
		domain.insertPlain("Room", 
				"address", "Sofia, OK",
				"roomName", "Second",
				"width", 5.0,
				"breadth", 5.0,
				"type", RoomType.LIVING_ROOM
				);
		domain.insertPlain("Room", 
				"address", "Sofia, ML",
				"roomName", "Main",
				"width", 5.0,
				"breadth", 5.0,
				"type", RoomType.LIVING_ROOM
		);
		
		domain.insertPlain("Floor",
				"address", "Sofia, OK",
				"roomName", "Main",
				"floor", 1);
		domain.insertPlain("Floor",
				"address", "Sofia, ML",
				"roomName", "Main",
				"floor", 1);
		
		domain.deletePlain("Room", "roomName", "Second");
		
		domain.insertPlain("Commission",
				"priceBand", PriceBand.PREMIUM,
				"areaCode", AreaCode.CITY,
				"saleSpeed", SpeedBand.MEDIUM,
				"commission", 3000.0);
		
		domain.commit();
		
		System.out.println("Property: " + domain.select("Property"));
		System.out.println("Offer: " + domain.select("Offer"));
		System.out.println("Decision: " + domain.select("Decision"));
		System.out.println("Room: " + domain.select("Room"));
		System.out.println("Floor: " + domain.select("Floor"));
		System.out.println();
		
		System.out.println("RoomInfo: " + domain.select("RoomInfo"));
		System.out.println("Acceptance: " + domain.select("Acceptance"));
		System.out.println("Rejection: " + domain.select("Rejection"));
		System.out.println("PropertyInfo: " + domain.select("PropertyInfo"));
		System.out.println("CurrentOffer: " + domain.select("CurrentOffer"));
		System.out.println("RawSales: " + domain.select("RawSales"));
		System.out.println("SoldProperty: " + domain.select("SoldProperty"));
		System.out.println("UnsoldProperty: " + domain.select("UnsoldProperty"));
		System.out.println("SalesInfo: " + domain.select("SalesInfo"));
		System.out.println("SalesCommissions: " + domain.select("SalesCommissions"));
		System.out.println();
		
		System.out.println("OpenOffers: " + domain.select("OpenOffers"));
		System.out.println("PropertyForWebSite: " + domain.select("PropertyForWebSite"));
		System.out.println("CommissionDue: " + domain.select("CommissionDue"));
		
		domain.begin();
		domain.insertPlain("Property", 
				"address", "Sofia, ML1",
				"price", 90000.0,
				"photo", null,
				"agent", "agent1",
				"dateRegistered", offerDate);
		try {
			domain.commit();
		} catch(ConstraintViolationException exc) {
			System.out.println("Rolledback");
		}
		System.out.println("Property: " + domain.select("Property"));
		
//		System.out.println();
//		System.out.println();
//		System.out.println(domain.join("CurrentOffer", domain.project("Property", "address", "agent", "dateRegistered")).select());
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
				.foreignKey(dom.resolveBase("Property"), "address");
		
		dom.relation("Decision",
				new Attribute("address", address),
				new Attribute("offerDate", Types.DATE),
				new Attribute("bidderName", name),
				new Attribute("bidderAddress", address),
				new Attribute("decisionDate", Types.DATE),
				new Attribute("accepted", Types.BOOL))
				.primaryKey("address", "offerDate", "bidderName", "bidderAddress")
				.foreignKey(dom.resolveBase("Offer"), "address", "offerDate", "bidderName", "bidderAddress");

		dom.relation("Room",
				new Attribute("address", address),
				new Attribute("roomName", Types.STRING),
				new Attribute("width", Types.DOUBLE),
				new Attribute("breadth", Types.DOUBLE),
				new Attribute("type", roomType))
				.primaryKey("address", "roomName")
				.foreignKey(dom.resolveBase("Property"), "address");

		dom.relation("Floor",
				new Attribute("address", address),
				new Attribute("roomName", Types.STRING),
				new Attribute("floor", Types.INT))
				.primaryKey("address", "roomName")
				.foreignKey(dom.resolveBase("Property"), "address")
				.foreignKey(dom.resolveBase("Room"), "address", "roomName");
		
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
		CalculatedAttribute priceBandPriceAtt = new CalculatedAttribute("priceBand", priceBand) {
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
		CalculatedAttribute priceBandOfferAtt = new CalculatedAttribute("priceBand", priceBand) {
			@Override
			public Object calculate(Entity entity) {
				double price = (Double) entity.getValue("offerPrice");
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
				return SpeedBand.MEDIUM; // TODO impl sometime
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
		
		dom.extend("PropertyInfo", "Property", priceBandPriceAtt, areaCodeAtt, numberOfRoomsAtt, squareFeetAtt);
		
		dom.summarize("CurrentOffer", "Offer", dom.project("Offer", "address", "bidderName", "bidderAddress"), new Aggregate() {
			@Override
			public Entity aggregate(Entity base, Set<Entity> entities) {
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
		
		dom.project("SoldProperty", dom.resolve("RawSales"), "address");
		
		dom.minus("UnsoldProperty", dom.project("Property", "address"), "SoldProperty");
		
		dom.project("SalesInfo", dom.extend("RawSales", areaCodeAtt, saleSpeedAtt, priceBandOfferAtt),
			"address", "agent", "areaCode", "saleSpeed", "priceBand");
		
		dom.project("SalesCommissions", dom.join("SalesInfo", "Commission"), "address", "agent", "commission");
		
		// External
		
		dom.join("OpenOffers", "CurrentOffer", dom.minus(
				dom.project_away(dom.resolve("CurrentOffer"), "offerPrice"),
				dom.project_away(dom.resolve("Decision"), "accepted", "decisionDate")));
		
		dom.project("PropertyForWebSite", dom.join("UnsoldProperty", "PropertyInfo"), "address", "price", "photo", "numberOfRooms", "squareFeet");
		
		dom.project("CommissionDue", dom.summarize("SalesCommissions", dom.project("SalesCommissions", "agent"), new Aggregate() {
			@Override
			public Entity aggregate(Entity base, Set<Entity> entities) {
				Double total = CollectionUtils.sumDouble(entities, new Function1<Double, Entity>() {
					@Override
					public Double execute(Entity p) {
						return (Double) p.getValue("commission");
					}});
				
				TreeSet<AttributeValue> values = new TreeSet<AttributeValue>(base.getValues());
				values.add(new AttributeValue(new Attribute("totalCommission", Types.DOUBLE), total));
				return new Entity(values);
			}}), "agent", "totalCommission");
		
		// Integrity
		dom.addCheck("All properties at least one room", new IntegrityCheck() {
			Relation intRelation = dom.restrict("PropertyInfo", new Condition() {
				@Override
				public boolean satisfied(Entity e) {
					int rooms = (Integer) e.getValue("numberOfRooms");
					return rooms < 1;
				}});
			
			@Override
			public boolean perform() {
				return intRelation.select().size() == 0;
			}
		});
		
		return dom;
	}
}
