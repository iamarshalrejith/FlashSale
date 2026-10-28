
import { useEffect, useRef } from "react";

import Navbar from "./components/Navbar";
import Hero from "./components/Hero";
import ProductCard from "./components/ProductCard";
import OrderStatus from "./components/OrderStatus";
import Footer from "./components/Footer";
import { useCheckout } from "./hooks/useCheckout";
import "./App.css";

export default function App() {
  const {
    order,
    loading,
    message,
    error,
    buyNow,
  } = useCheckout();

  const orderStatusRef = useRef(null);

  useEffect(() => {
    if (order) {
      orderStatusRef.current?.scrollIntoView({
        behavior: "smooth",
        block: "start",
      });
    }
  }, [order]);

  return (
    <main className="page">
      <Navbar />

      <Hero />

      <ProductCard
        loading={loading}
        order={order}
        onBuyNow={buyNow}
      />

      <div ref={orderStatusRef} id="order-status">
        <OrderStatus
          order={order}
          message={message}
          error={error}
        />
      </div>

      <Footer />
    </main>
  );
}
