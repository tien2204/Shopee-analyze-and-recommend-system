    // script.js
    const API_BASE_URL = "http://localhost:5000";
    const ITEMS_PER_PAGE_FRONTEND = 10; // Nên đồng bộ với backend hoặc lấy từ response
    let currentPage = 1;
    let currentKeyword = ""; // Lưu từ khóa search hiện tại để phân trang cho search

    // Hàm để hiển thị chi tiết sản phẩm chính (giữ nguyên)
    function displayMainProduct(product) {
        const mainProductSection = document.getElementById('mainProductSection');
        const mainProductImage = document.getElementById('mainProductImage');
        const mainProductTitle = document.getElementById('mainProductTitle');
        const mainProductDescription = document.getElementById('mainProductDescription');
        const mainProductPrice = document.getElementById('mainProductPrice');
        const mainProductASIN = document.getElementById('mainProductASIN');

        if (mainProductSection && product) {
            mainProductImage.src = product.image_url || 'https://via.placeholder.com/256?text=No+Image';
            mainProductImage.alt = product.name || 'Product Image';
            mainProductTitle.innerText = product.name || 'N/A';
            mainProductDescription.innerText = product.description || 'No description available.';
            mainProductPrice.innerText = product.price ? `${product.price.toLocaleString('vi-VN')} VND` : 'N/A';
            mainProductASIN.innerText = `ASIN: ${product.product_id || product.recommended_asin || 'N/A'}`;
            mainProductSection.style.display = 'block';
        } else if (mainProductSection) {
            mainProductSection.style.display = 'none';
        }
    }

    // Hàm mới để fetch sản phẩm (cho cả initial load và search, hỗ trợ phân trang)
    async function fetchProducts(page = 1, keyword = "") {
        const productListTitle = document.getElementById('productListTitle');
        const recommendationsContainer = document.getElementById('recommendationsContainer');
        const recommendationsTitle = document.getElementById('recommendationsTitle');
        const productListContainer = document.getElementById('productListContainer');
        const paginationControls = document.getElementById('paginationControls');

        if (recommendationsContainer) recommendationsContainer.innerHTML = '';
        if (recommendationsTitle) recommendationsTitle.style.display = 'none';
        displayMainProduct(null);

        let url = `${API_BASE_URL}/products?page=${page}&limit=${ITEMS_PER_PAGE_FRONTEND}`;
        if (keyword) {
            url = `${API_BASE_URL}/search?keyword=${encodeURIComponent(keyword)}&page=${page}&limit=${ITEMS_PER_PAGE_FRONTEND}`;
            if (productListTitle) productListTitle.innerText = `Search Results for: "${keyword}"`;
        } else {
            if (productListTitle) productListTitle.innerText = "Featured Products:";
        }
        
        currentKeyword = keyword; // Lưu lại keyword cho phân trang
        currentPage = page; // Lưu lại trang hiện tại

        try {
            if (productListContainer) productListContainer.innerHTML = '<p class="col-span-full text-center">Loading products...</p>';
            if (paginationControls) paginationControls.innerHTML = ''; // Xóa controls cũ

            const response = await fetch(url);
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status} - ${await response.text()}`);
            
            const data = await response.json(); // API giờ trả về object { products: [], total_pages: X, ... }
            
            renderProducts(data.products, 'productListContainer', false);
            renderPaginationControls(data.total_pages, data.current_page);

        } catch (error) {
            console.error("Error fetching products:", error);
            if (productListContainer) productListContainer.innerHTML = `<p class='col-span-full text-center text-red-500'>Error loading products: ${error.message}</p>`;
        }
    }


    // 1. Lấy và hiển thị sản phẩm ban đầu
    function loadInitialProducts() {
        fetchProducts(1); // Tải trang 1
    }
  
    // 2. Search sản phẩm theo keyword
    function searchProductsAPI() {
        const keyword = document.getElementById("keyword").value;
        if (!keyword.trim()) {
            const productListContainer = document.getElementById('productListContainer');
            if(productListContainer) productListContainer.innerHTML = "<p class='col-span-full text-center'>Please enter a keyword to search.</p>";
            document.getElementById('paginationControls').innerHTML = '';
            return;
        }
        fetchProducts(1, keyword); // Search luôn bắt đầu từ trang 1
    }
  
    // 3. Dựng HTML cho danh sách sản phẩm (giữ nguyên, chỉ cần đảm bảo product.product_id là ASIN)
    function renderProducts(items, containerId, isRecommendationList) {
      const container = document.getElementById(containerId);
      if (!container) {
        console.error(`Container with id '${containerId}' not found!`);
        return;
      }
      container.innerHTML = ''; 

      if (!Array.isArray(items) || items.length === 0) {
        const message = isRecommendationList ? 'No similar products found for the selected item.' : 'No products found.';
        container.innerHTML = `<p class="col-span-full text-center text-gray-500">${message}</p>`;
        return;
      }

      items.forEach(product => {
        const productDiv = document.createElement('div');
        productDiv.className = 'product-card'; 
        
        const asinForEvent = isRecommendationList ? product.recommended_asin : product.product_id;
        const displayName = product.name || "N/A";
        const displayImageUrl = product.image_url || 'https://via.placeholder.com/150?text=No+Image';
        const displayPrice = product.price ? `${product.price.toLocaleString('vi-VN')} VND` : '';
        const displaySimilarity = isRecommendationList && typeof product.similarity === 'number' ? `Similarity: ${product.similarity.toFixed(4)}` : '';

        productDiv.innerHTML = `
          <img src="${displayImageUrl}" alt="${displayName}">
          <h3>${displayName}</h3>
          ${displayPrice ? `<p class="product-price">${displayPrice}</p>` : ''}
          ${displaySimilarity ? `<p class="product-similarity">${displaySimilarity}</p>` : ''}
        `;

        if (asinForEvent) {
          productDiv.onclick = () => {
            let mainProductData = { 
                product_id: asinForEvent,
                name: displayName,
                price: product.price, 
                image_url: displayImageUrl,
                description: product.description || (isRecommendationList ? "Description not available for this recommended item." : "No description.")
            };
            displayMainProduct(mainProductData);
            showRecommendationsAPI(asinForEvent);
          };
        }
        container.appendChild(productDiv);
      });
    }
  
    // 4. Gọi API recommend và hiển thị kết quả (giữ nguyên)
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
        if (!response.ok) {
            const errorData = await response.json().catch(() => response.text());
            console.error("Backend error data:", errorData);
            throw new Error(`HTTP error! status: ${response.status} - ${typeof errorData === 'string' ? errorData : (errorData.message || errorData.error || 'Unknown backend error')}`);
        }
        const recommendedItems = await response.json();
        renderProducts(recommendedItems, 'recommendationsContainer', true);
      } catch (error) {
        console.error(`Error fetching recommendations for ${asin}:`, error);
        if (recommendationsContainer) recommendationsContainer.innerHTML = `<p class='col-span-full text-center text-red-500'>Error loading recommendations: ${error.message}</p>`;
      }
    }

    // 5. Hàm mới để render các nút phân trang
    function renderPaginationControls(totalPages, currentPage) {
        const paginationContainer = document.getElementById('paginationControls');
        if (!paginationContainer) return;
        paginationContainer.innerHTML = ''; // Xóa controls cũ

        if (totalPages <= 1) return; // Không cần phân trang nếu chỉ có 1 trang

        const maxPagesToShow = 5; // Số lượng nút trang tối đa hiển thị (không tính <<, >>, ...)
        let startPage, endPage;

        if (totalPages <= maxPagesToShow) {
            startPage = 1;
            endPage = totalPages;
        } else {
            if (currentPage <= Math.ceil(maxPagesToShow / 2)) {
                startPage = 1;
                endPage = maxPagesToShow;
            } else if (currentPage + Math.floor(maxPagesToShow / 2) >= totalPages) {
                startPage = totalPages - maxPagesToShow + 1;
                endPage = totalPages;
            } else {
                startPage = currentPage - Math.floor(maxPagesToShow / 2);
                endPage = currentPage + Math.floor(maxPagesToShow / 2);
            }
        }
        
        // Nút "<<" (Về trang đầu)
        if (currentPage > 1) {
            const firstPageButton = document.createElement('button');
            firstPageButton.innerText = '<<';
            firstPageButton.onclick = () => fetchProducts(1, currentKeyword);
            firstPageButton.className = 'pagination-button';
            paginationContainer.appendChild(firstPageButton);
        }

        // Nút "..." nếu startPage > 1
        if (startPage > 1) {
            const ellipsisStart = document.createElement('span');
            ellipsisStart.innerText = '...';
            ellipsisStart.className = 'pagination-ellipsis';
            paginationContainer.appendChild(ellipsisStart);
        }

        // Các nút số trang
        for (let i = startPage; i <= endPage; i++) {
            const pageButton = document.createElement('button');
            pageButton.innerText = i;
            pageButton.onclick = () => fetchProducts(i, currentKeyword);
            pageButton.className = 'pagination-button';
            if (i === currentPage) {
                pageButton.classList.add('active'); // Đánh dấu trang hiện tại
            }
            paginationContainer.appendChild(pageButton);
        }

        // Nút "..." nếu endPage < totalPages
        if (endPage < totalPages) {
            const ellipsisEnd = document.createElement('span');
            ellipsisEnd.innerText = '...';
            ellipsisEnd.className = 'pagination-ellipsis';
            paginationContainer.appendChild(ellipsisEnd);
        }
        
        // Nút ">>" (Đến trang cuối)
        if (currentPage < totalPages) {
            const lastPageButton = document.createElement('button');
            lastPageButton.innerText = '>>';
            lastPageButton.onclick = () => fetchProducts(totalPages, currentKeyword);
            lastPageButton.className = 'pagination-button';
            paginationContainer.appendChild(lastPageButton);
        }
    }

    window.onload = loadInitialProducts;
    