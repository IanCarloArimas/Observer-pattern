import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;



enum Category { POLITICS, SPORTS, TECH, BUSINESS, WORLD, ENTERTAINMENT }

final class NewsItem {
    private final String headline;
    private final String body;
    private final Category category;
    private final Instant publishedAt;

    NewsItem(String headline, String body, Category category) {
        this.headline = Objects.requireNonNull(headline);
        this.body = Objects.requireNonNull(body);
        this.category = Objects.requireNonNull(category);
        this.publishedAt = Instant.now();
    }

    String getHeadline()      { return headline; }
    String getBody()          { return body; }
    Category getCategory()    { return category; }
    Instant getPublishedAt()  { return publishedAt; }

    @Override public String toString() {
        return "[" + category + "] " + headline;
    }
}

interface Subscriber {
    String getId();
    void onNews(NewsItem news);  
}

interface NewsPublisher {
    void subscribe(Subscriber subscriber, Set<Category> categories);
    void subscribe(Subscriber subscriber);               
    void unsubscribe(Subscriber subscriber);
    void updatePreferences(Subscriber subscriber, Set<Category> categories);
    void publish(NewsItem news);
}



class NewsAgency implements NewsPublisher {


    private final ConcurrentMap<String, Subscription> subscriptions = new ConcurrentHashMap<>();
    private final ExecutorService dispatcher;

    NewsAgency(int threads) {
        this.dispatcher = Executors.newFixedThreadPool(threads);
    }

    @Override
    public void subscribe(Subscriber subscriber) {
        subscribe(subscriber, EnumSet.allOf(Category.class));
    }

    @Override
    public void subscribe(Subscriber subscriber, Set<Category> categories) {
        subscriptions.put(subscriber.getId(), new Subscription(subscriber, categories));
    }

    @Override
    public void unsubscribe(Subscriber subscriber) {
        subscriptions.remove(subscriber.getId());
    }

    @Override
    public void updatePreferences(Subscriber subscriber, Set<Category> categories) {

        subscriptions.computeIfPresent(subscriber.getId(),
                (id, old) -> new Subscription(old.subscriber, categories));
    }

    @Override
    public void publish(NewsItem news) {

        for (Subscription sub : subscriptions.values()) {
            if (!sub.wants(news.getCategory())) continue;
            dispatcher.submit(() -> {
                try {
                    sub.subscriber.onNews(news);
                } catch (Exception e) {
                
                    System.err.println("Delivery failed for " + sub.subscriber.getId()
                            + ": " + e.getMessage());
                }
            });
        }
    }

    void shutdown() throws InterruptedException {
        dispatcher.shutdown();
        dispatcher.awaitTermination(5, TimeUnit.SECONDS);
    }

    
    private static final class Subscription {
        final Subscriber subscriber;
        final Set<Category> categories;

        Subscription(Subscriber subscriber, Set<Category> categories) {
            this.subscriber = subscriber;
            this.categories = categories.isEmpty()
                    ? Collections.emptySet()
                    : Collections.unmodifiableSet(EnumSet.copyOf(categories));
        }

        boolean wants(Category c) { return categories.contains(c); }
    }
}


class EmailSubscriber implements Subscriber {
    private final String email;
    EmailSubscriber(String email) { this.email = email; }

    @Override public String getId() { return "email:" + email; }

    @Override public void onNews(NewsItem news) {
        System.out.printf("Email to %s -> %s%n", email, news);
    }
}

class MobileAppSubscriber implements Subscriber {
    private final String deviceId;
    MobileAppSubscriber(String deviceId) { this.deviceId = deviceId; }

    @Override public String getId() { return "mobile:" + deviceId; }

    @Override public void onNews(NewsItem news) {
        System.out.printf("Push to device %s -> %s%n", deviceId, news.getHeadline());
    }
}

// Sample Code

public class NewsServiceDemo {
    public static void main(String[] args) throws InterruptedException {
        NewsAgency agency = new NewsAgency(4);

        Subscriber ICBalajadia = new EmailSubscriber("ICBalajadia@neu.com.ph");
        Subscriber JEsperanza   = new MobileAppSubscriber("device-69");
        Subscriber BSCS1 = new EmailSubscriber("BSCS1@neu.com.ph");

        agency.subscribe(ICBalajadia);                                         // everything
        agency.subscribe(JEsperanza, EnumSet.of(Category.TECH, Category.SPORTS));
        agency.subscribe(BSCS1, EnumSet.of(Category.POLITICS));

        agency.publish(new NewsItem("Chip breakthrough announced", "...", Category.TECH));
        agency.publish(new NewsItem("Election results in", "...", Category.POLITICS));


        agency.unsubscribe(ICBalajadia);
        agency.updatePreferences(JEsperanza, EnumSet.of(Category.POLITICS));

        agency.publish(new NewsItem("Parliament passes bill", "...", Category.POLITICS));

        agency.shutdown();
    }
}