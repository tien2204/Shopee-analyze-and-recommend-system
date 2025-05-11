// script.js
const API_BASE_URL = "http://localhost:5000"; // Địa chỉ backend API

// 1. Tải và hiển thị các sản phẩm ban đầu khi trang được load
async function loadInitialProducts() {
  const productListTitle = document.getElementById('productListTitle');
  const recommendationsContainer = document.getElementById('recommendationsContainer');
  const recommendationsTitle = document.getElementById('recommendationsTitle');

  if (productListTitle) productListTitle.innerText = "Featured Products:";
  if (recommendationsContainer) recommendationsContainer.innerHTML = ''; // Xóa gợi ý cũ
  if (recommendationsTitle) recommendationsTitle.style.display = 'none'; // Ẩn tiêu đề gợi ý

  try {
    const response = await fetch(`${API_BASE_URL}/products?limit=10`); // Gọi API /products
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
    const products = await response.json();
    renderProducts(products, 'productListContainer', false); // false = không phải danh sách gợi ý
  } catch (error) {
    console.error("Error loading initial products:", error);
    const container = document.getElementById('productListContainer');
    if (container) container.innerHTML = "<p class='col-span-full text-center text-red-500'>Error loading products.</p>";
  }
}

// 2. Tìm kiếm sản phẩm theo keyword
async function searchProductsAPI() {
  const keyword = document.getElementById("keyword").value;
  const productListTitle = document.getElementById('productListTitle');
  const recommendationsContainer = document.getElementById('recommendationsContainer');
  const recommendationsTitle = document.getElementById('recommendationsTitle');

  if (productListTitle) productListTitle.innerText = `Search Results for: "${keyword}"`;
  if (recommendationsContainer) recommendationsContainer.innerHTML = ''; // Xóa gợi ý cũ
  if (recommendationsTitle) recommendationsTitle.style.display = 'none'; // Ẩn tiêu đề gợi ý

  const productListContainer = document.getElementById('productListContainer');
  if (!productListContainer) return;

  if (!keyword.trim()) {
    productListContainer.innerHTML = "<p class='col-span-full text-center'>Please enter a keyword to search.</p>";
    return;
  }

  try {
    productListContainer.innerHTML = '<p class="col-span-full text-center">Searching...</p>';
    const response = await fetch(`${API_BASE_URL}/search?keyword=${encodeURIComponent(keyword)}`);
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
    const data = await response.json();
    renderProducts(data, 'productListContainer', false);
  } catch (error) {
    console.error("Error searching products:", error);
    productListContainer.innerHTML = "<p class='col-span-full text-center text-red-500'>Error searching products.</p>";
  }
}

// 3. Dựng (render) HTML cho danh sách sản phẩm
function renderProducts(items, containerId, isRecommendationList) {
  const container = document.getElementById(containerId);
  if (!container) {
    console.error(`Container with id '${containerId}' not found!`);
    return;
  }
  container.innerHTML = ''; // Xóa nội dung cũ

  if (!Array.isArray(items) || items.length === 0) {
    const message = isRecommendationList ? 'No similar products found.' : 'No products found for your query.';
    container.innerHTML = `<p class="col-span-full text-center text-gray-500">${message}</p>`;
    return;
  }

  items.forEach(product => {
    const productDiv = document.createElement('div');
    productDiv.className = 'border rounded-lg p-3 shadow-md hover:shadow-xl transition-shadow duration-300 ease-in-out cursor-pointer flex flex-col items-center text-center';

    // Backend app.py của bạn trả về các trường:
    // - Từ /products và /search: { product_id (là ASIN), name (là title), price (là final_price), image_url }
    // - Từ /recommend: [{ recommended_asin, name, price, image_url, similarity }, ...]

    const asinToRecommend = isRecommendationList ? product.recommended_asin : product.product_id;
    const displayName = product.name || "N/A";
    const displayImageUrl = product.image_url || 'https://via.placeholder.com/150?text=No+Image'; // Placeholder
    const displayPrice = product.price ? `${product.price.toLocaleString('vi-VN')} VND` : '';
    const displaySimilarity = isRecommendationList && typeof product.similarity === 'number' ? `Similarity: ${product.similarity.toFixed(4)}` : '';

    productDiv.innerHTML = `
      <img src="${displayImageUrl}" alt="${displayName}" class="w-32 h-32 object-contain mb-2 rounded">
      <h3 class="font-semibold text-sm mt-1 h-10 overflow-hidden" title="${displayName}">${displayName}</h3>
      ${displayPrice ? `<p class="text-sm font-bold text-red-600 mt-1">${displayPrice}</p>` : ''}
      ${displaySimilarity ? `<p class="text-xs text-blue-500 mt-1">${displaySimilarity}</p>` : ''}
    `;

    if (asinToRecommend) {
      productDiv.onclick = () => showRecommendationsAPI(asinToRecommend);
    }
    container.appendChild(productDiv);
  });
}

// 4. Gọi API recommend và hiển thị kết quả
async function showRecommendationsAPI(asin) {
  const recommendationsContainer = document.getElementById('recommendationsContainer');
  const recommendationsTitle = document.getElementById('recommendationsTitle');

  if (!asin) {
    console.error("ASIN is undefined, cannot fetch recommendations.");
    if (recommendationsContainer) recommendationsContainer.innerHTML = "<p class='col-span-full text-center text-red-500'>Cannot fetch recommendations: Product ID is missing.</p>";
    return;
  }

  if (recommendationsTitle) recommendationsTitle.style.display = 'block';
  if (recommendationsContainer) recommendationsContainer.innerHTML = '<p class="col-span-full text-center">Loading recommendations...</p>';

  try {
    const response = await fetch(`${API_BASE_URL}/recommend/${asin}`);
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
    const recommendedItems = await response.json();
    renderProducts(recommendedItems, 'recommendationsContainer', true); // true = đây là danh sách gợi ý
  } catch (error)
{
    console.error(`Error fetching recommendations for ${asin}:`, error);
    if (recommendationsContainer) recommendationsContainer.innerHTML = "<p class='col-span-full text-center text-red-500'>Error loading recommendations.</p>";
  }
}

// Khi trang tải, gọi để hiển thị sản phẩm ban đầu
window.onload = loadInitialProducts;