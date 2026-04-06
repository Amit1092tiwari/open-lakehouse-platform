// Initialize Mermaid
mermaid.initialize({
  startOnLoad: true,
  theme: 'dark',
  themeVariables: {
    darkMode: true,
    primaryColor: '#4facfe',
    primaryTextColor: '#f8fafc',
    primaryBorderColor: '#334155',
    lineColor: '#64748b',
    secondaryColor: '#7c3aed',
    tertiaryColor: '#f59e0b',
    background: '#1e293b',
    mainBkg: '#1e293b',
    secondBkg: '#334155',
    textColor: '#f8fafc',
    fontSize: '14px',
  },
});

// Header scroll effect and progress bar
window.addEventListener('scroll', function () {
  const header = document.querySelector('.header');
  if (window.scrollY > 50) {
    header.classList.add('scrolled');
  } else {
    header.classList.remove('scrolled');
  }

  // Update scroll progress bar
  updateScrollProgress();
});

function updateScrollProgress() {
  const progressBar = document.querySelector('.scroll-progress');
  if (!progressBar) return;

  const windowHeight = window.innerHeight;
  const documentHeight = document.documentElement.scrollHeight;
  const scrollTop = window.pageYOffset || document.documentElement.scrollTop;
  const scrollPercentage = (scrollTop / (documentHeight - windowHeight)) * 100;

  progressBar.style.width = Math.min(scrollPercentage, 100) + '%';
}

// Back to top button
const backToTopButton = document.querySelector('.back-to-top');

window.addEventListener('scroll', function () {
  if (window.scrollY > 300) {
    backToTopButton.classList.add('visible');
  } else {
    backToTopButton.classList.remove('visible');
  }
});

// Smooth scroll for navigation links
document.querySelectorAll('a[href^="#"]').forEach((anchor) => {
  anchor.addEventListener('click', function (e) {
    e.preventDefault();
    const target = document.querySelector(this.getAttribute('href'));
    if (target) {
      const headerOffset = 80;
      const elementPosition = target.getBoundingClientRect().top;
      const offsetPosition = elementPosition + window.pageYOffset - headerOffset;

      window.scrollTo({
        top: offsetPosition,
        behavior: 'smooth',
      });
    }
  });
});

// Copy code functionality
function copyCode(button) {
  const codeBlock = button.closest('.code-block').querySelector('code');
  const code = codeBlock.textContent;

  navigator.clipboard
    .writeText(code)
    .then(() => {
      const originalText = button.textContent;
      button.textContent = '✓ Copied!';
      button.style.background = 'rgba(16, 185, 129, 0.3)';

      setTimeout(() => {
        button.textContent = originalText;
        button.style.background = '';
      }, 2000);
    })
    .catch((err) => {
      console.error('Failed to copy:', err);
      button.textContent = '✗ Failed';
      setTimeout(() => {
        button.textContent = '📋 Copy';
      }, 2000);
    });
}

// Animate elements on scroll
const observerOptions = {
  threshold: 0.1,
  rootMargin: '0px 0px -50px 0px',
};

const observer = new IntersectionObserver((entries) => {
  entries.forEach((entry) => {
    if (entry.isIntersecting) {
      entry.target.classList.add('fade-in');
    }
  });
}, observerOptions);

// Observe all sections
document.querySelectorAll('.section').forEach((section) => {
  observer.observe(section);
});

// Observe all cards
document.querySelectorAll('.feature-card, .deployment-card, .use-case-card').forEach((card) => {
  observer.observe(card);
});

// Dynamic stats counter animation
function animateCounter(element, target, duration = 2000) {
  let start = 0;
  const increment = target / (duration / 16);
  const timer = setInterval(() => {
    start += increment;
    if (start >= target) {
      element.textContent = target + (element.dataset.suffix || '');
      clearInterval(timer);
    } else {
      element.textContent = Math.floor(start) + (element.dataset.suffix || '');
    }
  }, 16);
}

// Animate stat numbers when they come into view
const statsObserver = new IntersectionObserver(
  (entries) => {
    entries.forEach((entry) => {
      if (entry.isIntersecting && !entry.target.classList.contains('counted')) {
        entry.target.classList.add('counted');
        const statNumber = entry.target.querySelector('.stat-number');
        const target = parseInt(statNumber.dataset.count);
        animateCounter(statNumber, target);
      }
    });
  },
  { threshold: 0.5 },
);

document.querySelectorAll('.stat-item').forEach((stat) => {
  statsObserver.observe(stat);
});

// Mobile menu toggle (if needed)
const createMobileMenu = () => {
  const nav = document.querySelector('.nav-menu');
  const navContainer = document.querySelector('.nav-container');

  if (window.innerWidth <= 768 && !document.querySelector('.mobile-menu-toggle')) {
    const toggleButton = document.createElement('button');
    toggleButton.className = 'mobile-menu-toggle';
    toggleButton.innerHTML = '☰';
    toggleButton.style.cssText = `
            background: var(--gradient-3);
            border: none;
            color: white;
            font-size: 1.5rem;
            padding: 0.5rem 1rem;
            border-radius: 8px;
            cursor: pointer;
            display: block;
        `;

    toggleButton.addEventListener('click', () => {
      nav.style.display = nav.style.display === 'flex' ? 'none' : 'flex';
      if (nav.style.display === 'flex') {
        nav.style.cssText = `
                    display: flex;
                    flex-direction: column;
                    position: absolute;
                    top: 100%;
                    left: 0;
                    right: 0;
                    background: rgba(15, 23, 42, 0.98);
                    padding: 1rem;
                    box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
                `;
      }
    });

    navContainer.appendChild(toggleButton);
  }
};

// Handle responsive menu
window.addEventListener('resize', createMobileMenu);
createMobileMenu();

// Enhanced link tracking
document.querySelectorAll('a[href^="http"]').forEach((link) => {
  link.setAttribute('target', '_blank');
  link.setAttribute('rel', 'noopener noreferrer');
});

// Highlight active section in navigation
const sections = document.querySelectorAll('.section[id]');
const navLinks = document.querySelectorAll('.nav-link');

window.addEventListener('scroll', () => {
  let current = '';
  sections.forEach((section) => {
    const sectionTop = section.offsetTop;
    const sectionHeight = section.clientHeight;
    if (pageYOffset >= sectionTop - 100) {
      current = section.getAttribute('id');
    }
  });

  navLinks.forEach((link) => {
    link.classList.remove('active');
    if (link.getAttribute('href').substring(1) === current) {
      link.classList.add('active');
    }
  });
});

// Add ripple effect to buttons
document.querySelectorAll('.btn-primary, .btn-secondary, .cta-button').forEach((button) => {
  button.addEventListener('click', function (e) {
    const ripple = document.createElement('span');
    const rect = this.getBoundingClientRect();
    const size = Math.max(rect.width, rect.height);
    const x = e.clientX - rect.left - size / 2;
    const y = e.clientY - rect.top - size / 2;

    ripple.style.cssText = `
            width: ${size}px;
            height: ${size}px;
            left: ${x}px;
            top: ${y}px;
            position: absolute;
            border-radius: 50%;
            background: rgba(255, 255, 255, 0.3);
            transform: scale(0);
            animation: ripple 0.6s ease-out;
            pointer-events: none;
        `;

    this.style.position = 'relative';
    this.style.overflow = 'hidden';
    this.appendChild(ripple);

    setTimeout(() => ripple.remove(), 600);
  });
});

// Add CSS for ripple animation
const style = document.createElement('style');
style.textContent = `
    @keyframes ripple {
        to {
            transform: scale(4);
            opacity: 0;
        }
    }
    
    .active {
        color: var(--primary-color) !important;
    }
    
    .active::after {
        width: 100% !important;
    }
`;
document.head.appendChild(style);

// Performance optimization: Lazy load diagrams
const diagramObserver = new IntersectionObserver(
  (entries) => {
    entries.forEach((entry) => {
      if (entry.isIntersecting) {
        const diagram = entry.target;
        if (diagram.classList.contains('mermaid') && !diagram.dataset.processed) {
          mermaid.init(undefined, diagram);
          diagram.dataset.processed = 'true';
        }
      }
    });
  },
  { rootMargin: '100px' },
);

document.querySelectorAll('.mermaid').forEach((diagram) => {
  diagramObserver.observe(diagram);
});

// Easter egg: Konami code
let konamiCode = [];
const konamiPattern = [
  'ArrowUp',
  'ArrowUp',
  'ArrowDown',
  'ArrowDown',
  'ArrowLeft',
  'ArrowRight',
  'ArrowLeft',
  'ArrowRight',
  'b',
  'a',
];

document.addEventListener('keydown', (e) => {
  konamiCode.push(e.key);
  konamiCode = konamiCode.slice(-10);

  if (konamiCode.join(',') === konamiPattern.join(',')) {
    document.body.style.animation = 'rainbow 2s infinite';
    setTimeout(() => {
      document.body.style.animation = '';
    }, 5000);
  }
});

const rainbowStyle = document.createElement('style');
rainbowStyle.textContent = `
    @keyframes rainbow {
        0% { filter: hue-rotate(0deg); }
        100% { filter: hue-rotate(360deg); }
    }
`;
document.head.appendChild(rainbowStyle);

// Analytics and tracking (placeholder)
function trackEvent(category, action, label) {
  if (typeof gtag !== 'undefined') {
    gtag('event', action, {
      event_category: category,
      event_label: label,
    });
  }
  console.log(`Event tracked: ${category} - ${action} - ${label}`);
}

// Track CTA clicks
document.querySelectorAll('.cta-button, .btn-primary').forEach((button) => {
  button.addEventListener('click', () => {
    trackEvent('CTA', 'Click', button.textContent);
  });
});

// Log page load time
window.addEventListener('load', () => {
  const loadTime = performance.timing.loadEventEnd - performance.timing.navigationStart;
  console.log(`Page loaded in ${loadTime}ms`);
  trackEvent('Performance', 'PageLoad', `${loadTime}ms`);
});

// Console message
console.log('%c🚀 End-to-End Data Pipeline Wiki', 'font-size: 20px; font-weight: bold; color: #4facfe;');
console.log('%cBuilt with ❤️ for data engineers and scientists', 'font-size: 12px; color: #cbd5e1;');
console.log('%cGitHub: https://github.com/hoangsonww/End-to-End-Data-Pipeline', 'font-size: 12px; color: #7c3aed;');
