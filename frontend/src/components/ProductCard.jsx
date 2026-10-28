
export default function ProductCard({ loading, order, onBuyNow }) {
  const isDisabled = loading || Boolean(order);

  return (
    <section className="purchase-panel">
      <div className="product-info">
        <span className="category">FEATURED PRODUCT</span>

        <h2>iPhone 15</h2>

        <p>Premium performance. Built for everything.</p>

        <div className="price-row">
          <span className="price">₹49,999</span>
          <span className="old-price">₹64,999</span>
          <span className="discount">23% OFF</span>
        </div>
      </div>

      <div className="buy-area">
        <button
          className="buy-button"
          type="button"
          onClick={onBuyNow}
          disabled={isDisabled}
        >
          {loading
            ? "RESERVING..."
            : order
              ? "ORDER PLACED"
              : "BUY NOW →"}
        </button>
      </div>
    </section>
  );
}
